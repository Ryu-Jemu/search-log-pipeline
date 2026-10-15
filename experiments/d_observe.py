"""실험 D: 관측(Prometheus·Grafana)과 경보 규칙이 의도적 장애에 반응하는지 확인한다.

day1을 60배속으로 재생하며 지표·탐지 DAG를 함께 돌리고, Prometheus 경보 상태를 2초마다 기록한다.
--no-faults : 장애 없이 평상시 값(lag, DLQ 비율 등)만 잰다(경보 임계값 근거).
기본(장애 있음, 모두 의도적 주입):
  1) 실험 B의 장애 지점 주입: 수집기를 정상 종료한 뒤 --failpoint-prob 1 --failpoint-where after로 다시 띄워
     첫 배치를 적재한 직후·커밋 전에 스스로 SIGKILL하게 한다. 그 뒤 DOWN_S초 동안 재시작하지 않는다
     (생산은 계속) → lag·수집기 down 경보. 이후 재시작(ClickHouse 오프셋 재개).
  2) 형식 오류 메시지(잘린 JSON)를 BURST_S초 동안 초당 BURST_RATE건 Kafka에 넣는다 → DLQ 비율 경보.
  3) 확정 후 늦은 도착: c2와 같은 방식(2·5·8시 확정 직후 5건).
끝난 뒤 재생기 정답과 대조(유실·중복), Prometheus 시계열·경보 기록, Grafana 대시보드 스크린샷을 남긴다.
"""
import argparse
import json
import os
import signal
import subprocess
import threading
import time
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone

from c2_metrics_dag import COMPOSE, SIM0, af, injector, psql
from common import ROOT, save, spawn, truth

from confluent_kafka import Consumer, Producer, TopicPartition
from slp import abuse, metrics
from slp.infra import BOOTSTRAP, DLQ, TOPIC, ch, create_events_table, reset_topics

DAGS = ("hourly_metrics", "abuse_detection")
PROM = "http://localhost:9090"
GROUP, INSTANCE, PORT = "slp-ingest", "ingest-0", "8000"
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"


def prom(path: str, **params) -> dict:
    url = f"{PROM}{path}?{urllib.parse.urlencode(params)}"
    return json.load(urllib.request.urlopen(url, timeout=10))


def reset_dag(dag: str) -> None:
    af("dags", "pause", dag)
    af("dags", "delete", dag, "-y")
    for _ in range(180):
        paused = psql(f"select is_paused from dag where dag_id = '{dag}'")
        runs = psql(f"select count(*) from dag_run where dag_id = '{dag}'")
        if paused and runs[0][0] == "0":
            return
        time.sleep(1)
    raise RuntimeError(f"{dag} 재생성 대기 시간 초과")


class AlertPoller(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.rows, self.stop_ev = [], threading.Event()

    def run(self):
        while not self.stop_ev.is_set():
            t = time.time()
            try:
                for a in prom("/api/v1/alerts")["data"]["alerts"]:
                    self.rows.append({"t": round(t, 2), "alert": a["labels"]["alertname"], "state": a["state"],
                                      "active_at": a.get("activeAt"), "value": a.get("value")})
            except Exception as e:  # noqa: BLE001
                self.rows.append({"t": round(t, 2), "error": str(e)[:200]})
            self.stop_ev.wait(2)


def collector(log: str, *extra: str):
    return spawn("slp.consume", "--group", GROUP, "--instance-id", INSTANCE, "--metrics-port", PORT, *extra,
                 log=os.path.join(ROOT, "data", log))


def dlq_burst(seconds: float, rate: float) -> int:
    """의도적 주입: 잘린 JSON(bad_json)을 초당 rate건."""
    p = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 20})
    n, t0 = 0, time.time()
    while time.time() - t0 < seconds:
        body = json.dumps({"event_id": str(uuid.uuid4()), "event_type": "search",
                           "event_time": "2026-01-05T06:30:00.000+00:00", "user_id": "u0000000"}).encode()[:-7]
        p.produce(TOPIC, value=body, key=f"inj{n % 97}")
        n += 1
        p.poll(0)
        time.sleep(max(0.0, t0 + n / rate - time.time()))
    p.flush()
    return n


def episodes(rows: list[dict]) -> dict:
    """경보별 pending·firing 구간(폴링 시각 기준). Prometheus에는 'resolved' 상태가 없어 마지막으로 보인 시각을 쓴다."""
    out: dict = {}
    for name in sorted({r["alert"] for r in rows if "alert" in r}):
        seen = [r for r in rows if r.get("alert") == name]
        eps, cur = [], None
        polls = sorted({r["t"] for r in rows if "t" in r})
        seen_t = {r["t"]: r["state"] for r in seen}
        for t in polls:
            st = seen_t.get(t)
            if st and cur is None:
                cur = {"first_seen": t, "first_pending": t if st == "pending" else None,
                       "first_firing": t if st == "firing" else None, "last_seen": t}
            elif st:
                cur["last_seen"] = t
                if st == "firing" and cur["first_firing"] is None:
                    cur["first_firing"] = t
            elif cur is not None:
                cur["gone_at"] = t
                eps.append(cur)
                cur = None
        if cur:
            eps.append(cur)
        out[name] = eps
    return out


def series(q: str, start: float, end: float) -> list:
    r = prom("/api/v1/query_range", query=q, start=start, end=end, step=5)["data"]["result"]
    return [{"metric": x["metric"], "values": [[float(t), v] for t, v in x["values"]]} for x in r]


def screenshot(path: str, start: float, end: float) -> str:
    url = (f"http://localhost:3000/d/slp-pipeline/search-log-pipeline?orgId=1&kiosk&timezone=utc"
           f"&from={int(start * 1000)}&to={int(end * 1000)}")
    r = subprocess.run([CHROME, "--headless=new", "--disable-gpu", "--hide-scrollbars", f"--screenshot={path}",
                        "--window-size=1600,1640", "--virtual-time-budget=25000", url],
                       capture_output=True, text=True, timeout=120)
    return url if os.path.exists(path) else f"실패: {r.stderr[-300:]}"


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=os.path.join(ROOT, "data", "day1.tsv"))
    ap.add_argument("--speed", type=float, default=60)
    ap.add_argument("--hours", type=int, default=12)
    ap.add_argument("--faults", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--kill-at-h", type=float, default=10.0, help="수집기 장애 시점(가상 시)")
    ap.add_argument("--down-s", type=float, default=60, help="수집기를 재시작하지 않는 시간(실제 초)")
    ap.add_argument("--burst-at-h", type=float, default=6.5, help="형식 오류 주입 시점(가상 시)")
    ap.add_argument("--burst-s", type=float, default=60)
    ap.add_argument("--burst-rate", type=float, default=30)
    ap.add_argument("--name", default="observability_mac_m4")
    ap.add_argument("--shot", default=os.path.join(ROOT, "docs", "img", "D-dashboard.png"))
    a = ap.parse_args()
    until = SIM0 + timedelta(hours=a.hours + 1, minutes=10)
    c = ch()
    reset_topics()
    create_events_table(c, "events")
    metrics.create_tables(c, drop=True)
    c.command(f"DROP TABLE IF EXISTS {abuse.DETECTIONS} SYNC")
    abuse.create_detections_table(c)
    for d in DAGS:
        reset_dag(d)
    log: dict = {"events": []}
    cons = collector("d_consumer_0.log")
    time.sleep(3)
    wall0 = time.time() + 3
    to_wall = lambda h: wall0 + h * 3600 / a.speed  # noqa: E731
    prod = spawn("slp.produce", a.data, "--speed", str(a.speed), "--sim0", SIM0.isoformat(), "--start-at",
                 str(wall0), "--until", until.isoformat(), log=os.path.join(ROOT, "data", "d_producer.log"))
    for d in DAGS:
        af("dags", "unpause", d)
    poller = AlertPoller()
    poller.start()
    injected: list[dict] = []
    threading.Thread(target=injector, args=(ch(), [2, 5, 8], 5, injected), daemon=True).start()
    burst_n = 0
    if a.faults:
        plan = sorted([(to_wall(a.kill_at_h), "kill"), (to_wall(a.burst_at_h), "burst")])
        for due, what in plan:
            time.sleep(max(0.0, due - time.time()))
            if what == "kill":
                log["events"].append({"t": time.time(), "what": "collector_sigterm"})
                cons.send_signal(signal.SIGTERM)
                cons.wait()
                fp = collector("d_consumer_failpoint.log", "--failpoint-prob", "1.0", "--failpoint-where", "after")
                log["events"].append({"t": time.time(), "what": "failpoint_collector_start"})
                fp.wait()
                fp_line = [x for x in open(os.path.join(ROOT, "data", "d_consumer_failpoint.log"))
                           if x.startswith("FAILPOINT")]
                log["events"].append({"t": time.time(), "what": "failpoint_collector_dead", "line": fp_line,
                                      "returncode": fp.returncode})
                time.sleep(a.down_s)
                cons = collector("d_consumer_1.log")
                log["events"].append({"t": time.time(), "what": "collector_restart"})
            else:
                log["events"].append({"t": time.time(), "what": "dlq_burst_start"})
                burst_n = dlq_burst(a.burst_s, a.burst_rate)
                log["events"].append({"t": time.time(), "what": "dlq_burst_end", "n": burst_n})
    deadline = time.time() + (a.hours + 2) * 3600 / a.speed + 900
    while time.time() < deadline:
        st = psql("select dag_id, state from dag_run where dag_id in ('hourly_metrics', 'abuse_detection')")
        if len(st) == 2 * a.hours and all(s[1] in ("success", "failed") for s in st):
            break
        time.sleep(5)
    prod.wait()
    time.sleep(90)  # lag 해소와 경보 해제를 기록하기 위해(경보 'for' + 1분 증가율 창)
    end = time.time()
    poller.stop_ev.set()
    with open(os.path.join(ROOT, "data", "d_raw_state.json"), "w") as f:
        json.dump({"wall0": wall0, "end": end, "log": log, "alerts": poller.rows, "injected": injected,
                   "burst_n": burst_n}, f)
    shot = screenshot(a.shot, wall0 - 60, end) if a.faults else None
    cons.send_signal(signal.SIGTERM)
    cons.wait()

    # ---------- 결과 ----------
    produced = json.loads(open(os.path.join(ROOT, "data", "d_producer.log")).read().strip().splitlines()[-1])
    tr = truth(a.data, limit=produced["produced"])
    inj_ids = {i for x in injected for i in x["event_ids"]}
    expect = tr["ids"] | inj_ids
    got = {str(r[0]) for r in c.query("SELECT event_id FROM events").result_rows}
    rows, uniq = c.query("SELECT count(), uniqExact(event_id) FROM events").result_rows[0]
    k = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "d-count"})
    dlq_lo, dlq_hi = k.get_watermark_offsets(TopicPartition(DLQ, 0), timeout=5)
    k.close()
    runs = psql("select dag_id, state, count(*) from dag_run where dag_id in ('hourly_metrics', 'abuse_detection') "
                "group by 1, 2 order by 1, 2")
    match = 0
    for h in range(a.hours):
        t = metrics.aggregate(c, SIM0 + timedelta(hours=h))
        lt = c.query(f"""SELECT searches, clicks, unique_users, zero_click_searches FROM {metrics.METRICS}_latest
                         WHERE hour = toDateTime('{(SIM0 + timedelta(hours=h)):%Y-%m-%d %H:%M:%S}', 'UTC')""").result_rows
        match += bool(lt) and tuple(lt[0]) == (t["searches"], t["clicks"], t["unique_users"], t["zero_click_searches"])
    late = c.query(f"SELECT toHour(hour), late_events, refinalized_version FROM {metrics.LATE} ORDER BY hour").result_rows
    det = c.query(f"SELECT rule, count() FROM {abuse.DETECTIONS} FINAL GROUP BY rule ORDER BY rule").result_rows
    offline = {}
    for h in range(a.hours):
        for r in abuse.detect_hour(c, "events", SIM0 + timedelta(hours=h)):
            offline[r["rule"]] = offline.get(r["rule"], 0) + 1
    s0, s1 = wall0 - 60, end
    lag_q = f'sum(kafka_consumergroup_lag{{topic="search-events", consumergroup="{GROUP}"}})'
    ratio_q = "sum(increase(slp_dlq_total[1m])) / sum(increase(slp_consumed_total[1m]))"
    ser = {"lag": series(lag_q, s0, s1), "consumed_rate": series("sum(rate(slp_consumed_total[30s]))", s0, s1),
           "inserted_rate": series("sum(rate(slp_inserted_rows_total[30s]))", s0, s1),
           "dlq_ratio_1m": series(ratio_q, s0, s1), "collector_up": series('up{job="collector"}', s0, s1),
           "alerts": series("ALERTS", s0, s1),
           "insert_p95_s": series("histogram_quantile(0.95, sum by (le) (rate(slp_insert_seconds_bucket[1m])))",
                                  s0, s1)}

    def vals(name, t_from=None, t_to=None):
        return [float(v) for x in ser[name] for t, v in x["values"] if v not in ("NaN", "+Inf")
                and (t_from is None or t >= t_from) and (t_to is None or t <= t_to)]
    first_fault = min([e["t"] for e in log["events"]] or [end])
    normal = {k2: {"max": max(vals(k2, wall0, first_fault - 5) or [0]), "n_samples": len(vals(k2, wall0, first_fault - 5))}
              for k2 in ("lag", "dlq_ratio_1m", "consumed_rate", "insert_p95_s")}
    res = {"speed": a.speed, "hours": a.hours, "faults": a.faults, "wall0": wall0, "end": end,
           "events": log["events"], "burst_messages": burst_n, "injected_late": injected,
           "alert_episodes": episodes(poller.rows), "alert_polls": len(poller.rows),
           "normal_before_first_fault": normal,
           "truth": {"produced": produced["produced"], "valid": tr["valid"], "rejected": tr["rejected"],
                     "injected_late": len(inj_ids), "rows": rows, "unique": uniq, "lost": len(expect - got),
                     "unknown": len(got - expect), "duplicates": rows - uniq,
                     "dlq_messages": dlq_hi - dlq_lo, "dlq_expected_min": tr["rejected"] + burst_n},
           "dag_runs": runs, "hourly_latest_matches_truth": match,
           "late_after_final": late, "abuse_detections": det, "abuse_offline_same_table": offline,
           "screenshot": shot, "series": ser, "rules_file": open(os.path.join(ROOT, "observability", "prometheus",
                                                                                "alerts.yml")).read()}
    print(save(a.name, res))
    print(json.dumps({k2: res[k2] for k2 in ("events", "alert_episodes", "normal_before_first_fault", "truth",
                                              "dag_runs", "hourly_latest_matches_truth", "abuse_detections",
                                              "abuse_offline_same_table", "screenshot")}, ensure_ascii=False,
                     indent=1, default=str))

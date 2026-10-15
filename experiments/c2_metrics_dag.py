"""실험 C2: 시간별 지표 DAG(워터마크 센서 + 버전 확정)를 배속 재생 위에서 실제로 돌린다.

실험 C의 설계(+1분 잠정 → +15분 확정, 확정 후 늦은 도착 감지·재확정)를 Airflow로 구현한 것을 검증한다.
- day1을 가상 시각 SPEED배속으로 재생한다(기본 60배: 가상 1시간 = 실제 1분).
  마지막 시간(11시)의 +60분 검사까지 워터마크가 가도록 13:10까지 보낸다.
- Airflow DAG `hourly_metrics`를 초기화한 뒤 재생 시작과 함께 언패즈한다. 진행은 워터마크만 결정한다.
- **의도적 주입:** INJECT_HOURS의 시간마다, 확정(v2)이 기록된 직후 그 시간 event_time을 가진 검색 이벤트
  N건을 Kafka로 보낸다(재생기 늦은 도착 상한 600초를 넘는 경로 시험). +60분 검사가 이를 세고 재확정해야 한다.
- 끝난 뒤 최종 적재본을 정답으로 v1·v2·최신 버전을 대조하고, Airflow 메타DB에서 태스크 시각을 읽어
  게이트(가상 시각) 대비 실제 계산 시점을 기록한다.
"""
import argparse
import json
import os
import statistics
import subprocess
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

from common import ROOT, save, spawn

from confluent_kafka import Producer
from slp import metrics
from slp.infra import BOOTSTRAP, TOPIC, ch, create_events_table, reset_topics

DAG = "hourly_metrics"
COMPOSE = ["docker", "compose", "-f", os.path.join(ROOT, "docker-compose.yml"), "--profile", "airflow"]
SIM0 = datetime(2026, 1, 5, tzinfo=timezone.utc)
GATES = {"provisional": 1, "finalize": 15, "late_after_final_check": 60}  # 시간 끝 + 가상 분


def af(*args: str) -> str:
    r = subprocess.run(COMPOSE + ["exec", "-T", "airflow", "airflow", *args], capture_output=True, text=True,
                       timeout=180)
    return r.stdout + r.stderr


def psql(sql: str) -> list[list[str]]:
    out = subprocess.run(COMPOSE + ["exec", "-T", "postgres", "psql", "-U", "airflow", "-At", "-F", "\t", "-c", sql],
                         capture_output=True, text=True, check=True, timeout=60).stdout
    return [line.split("\t") for line in out.strip().splitlines() if line]


def reset_dag() -> None:
    """이전 run을 지우고, DAG가 다시 파싱돼 일시정지 상태로 나타날 때까지 기다린다."""
    af("dags", "pause", DAG)
    af("dags", "delete", DAG, "-y")
    for _ in range(180):
        paused = psql(f"select is_paused from dag where dag_id = '{DAG}'")
        runs = psql(f"select count(*) from dag_run where dag_id = '{DAG}'")
        if paused and runs[0][0] == "0":
            return
        time.sleep(1)
    raise RuntimeError("DAG 재생성 대기 시간 초과")


def epoch(s: str | None) -> float | None:
    return datetime.fromisoformat(s).timestamp() if s else None


class Sampler(threading.Thread):
    """docker stats(컨테이너 메모리)를 주기적으로 기록한다."""

    def __init__(self, every: float = 15):
        super().__init__(daemon=True)
        self.every, self.rows, self.stop_ev = every, [], threading.Event()

    def run(self):
        while not self.stop_ev.is_set():
            out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}"], capture_output=True,
                                 text=True).stdout
            self.rows.append({"t": round(time.time(), 1),
                              **{json.loads(x)["Name"]: json.loads(x)["MemUsage"] for x in out.splitlines()}})
            self.stop_ev.wait(self.every)


def inject(hour: datetime, n: int) -> list[dict]:
    """의도적 주입: 그 시간 event_time을 가진 검색 이벤트 n건(형식은 재생기와 같다)."""
    p = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True})
    out = []
    for i in range(n):
        user = f"u{9_000_000 + hour.hour * 100 + i:07d}"
        e = {"event_id": str(uuid.uuid4()), "event_type": "search",
             "event_time": (hour + timedelta(minutes=5 + 10 * i)).isoformat(timespec="milliseconds"),
             "user_id": user, "session_id": f"sinj{hour.hour:02d}{i:02d}", "qid": 900000000 + i,
             "query": "slp very late injection test"}
        p.produce(TOPIC, value=json.dumps(e).encode(), key=user)
        out.append(e)
    p.flush()
    return out


def injector(c, hours: list[int], n: int, log: list):
    pending = list(hours)
    while pending:
        for h in list(pending):
            hs = SIM0 + timedelta(hours=h)
            if metrics.latest_final(c, hs) is not None:
                t = time.time()
                evs = inject(hs, n)
                log.append({"hour": h, "wall": t, "n": n, "event_ids": [e["event_id"] for e in evs]})
                pending.remove(h)
        time.sleep(0.2)


def collect(c, a, st: dict) -> dict:
    """재생이 끝난 뒤 결과를 모은다. st는 재생 중에만 얻을 수 있는 값(data/c2_raw_state.json)."""
    wall0, unpause_wall, injected = st["wall0"], st["unpause_wall"], st["injected"]
    q_start = datetime.fromisoformat(st["q_start"])
    until = SIM0 + timedelta(hours=a.hours + 1, minutes=10)
    to_sim = lambda w: SIM0 + timedelta(seconds=(w - wall0) * a.speed)  # noqa: E731
    # ---------- 결과 수집 ----------
    runs = psql(f"""select run_id, data_interval_start, state, start_date, end_date from dag_run
                    where dag_id = '{DAG}' order by data_interval_start""")
    tis = psql(f"""select r.data_interval_start, t.task_id, t.state, t.start_date, t.end_date, t.try_number
                   from task_instance t join dag_run r on r.dag_id = t.dag_id and r.run_id = t.run_id
                   where t.dag_id = '{DAG}' order by 1, t.start_date""")
    vers = c.query(f"""SELECT toHour(hour), version, stage, is_final, toUnixTimestamp64Milli(computed_at) / 1000,
                              toUnixTimestamp64Milli(watermark) / 1000, searches, clicks, unique_users,
                              zero_click_searches
                       FROM {metrics.VERSIONS} ORDER BY hour, version""").result_rows
    late = c.query(f"""SELECT toHour(hour), final_version, late_events, late_by_inserted_at, refinalized_version
                       FROM {metrics.LATE} ORDER BY hour, checked_at""").result_rows
    latest = {r[0]: r[1:] for r in c.query(f"""SELECT toHour(hour), version, searches, clicks, unique_users,
                                                    zero_click_searches FROM {metrics.METRICS}_latest""").result_rows}
    truth = {}
    for h in range(a.hours):
        t = metrics.aggregate(c, SIM0 + timedelta(hours=h))
        truth[h] = (t["searches"], t["clicks"], t["unique_users"], t["zero_click_searches"])

    per_hour = []
    for h in range(a.hours):
        hv = [v for v in vers if v[0] == h]
        row = {"hour": h, "truth": dict(zip(["searches", "clicks", "users", "zero_click"], truth[h])),
               "versions": [], "late_checks": [list(x[1:]) for x in late if x[0] == h],
               "latest_version": latest[h][0], "latest_matches_truth": tuple(latest[h][1:]) == truth[h]}
        n_inj = next((x["n"] for x in injected if x["hour"] == h), 0)
        for _, ver, stage, fin, cat, wm, s, k, u, z in hv:
            gate_min = {"provisional": 1, "final": 15, "refinal": 60}[stage]
            miss = (truth[h][0] + truth[h][1]) - (s + k)
            gate = SIM0 + timedelta(hours=h + 1, minutes=gate_min)
            row["versions"].append({
                "version": ver, "stage": stage, "is_final": fin, "searches": s, "clicks": k, "users": u,
                "zero_click": z, "missing_events": miss,
                # 주입은 v2 기록 뒤에 하므로 v1·v2 누락에는 주입분이 들어 있다. 재생기 스트림만의 누락을 따로 둔다.
                "missing_events_excl_injected": miss - n_inj if stage != "refinal" else miss,
                "missing_users": truth[h][2] - u,
                "complete_pct": round(100 * (s + k) / (truth[h][0] + truth[h][1]), 4),
                "computed_virtual_min_after_gate": round((to_sim(cat) - gate).total_seconds() / 60, 2),
                # 워터마크는 event_time 기준(가상 시각)이다
                "watermark_virtual_min_after_hour_end": (
                    round((wm - (SIM0 + timedelta(hours=h + 1)).timestamp()) / 60, 2) if wm else None)})
        per_hour.append(row)

    # 태스크 시각: 센서 성공(종료) → 계산 태스크 시작까지 실제 초, 계산 태스크 시작의 게이트 대비 가상 분
    task_rows = []
    for dis, task_id, state, t_st, t_en, tries in tis:
        h = datetime.fromisoformat(dis).astimezone(timezone.utc).hour
        rec = {"hour": h, "task": task_id, "state": state, "try": int(tries), "start": epoch(t_st),
               "end": epoch(t_en)}
        if task_id in GATES and t_st:
            gate = SIM0 + timedelta(hours=h + 1, minutes=GATES[task_id])
            rec["start_virtual_min_after_gate"] = round((to_sim(epoch(t_st)) - gate).total_seconds() / 60, 2)
        task_rows.append(rec)
    hand_off = []
    by = {(r["hour"], r["task"]): r for r in task_rows}
    for h in range(a.hours):
        for sensor, nxt in (("wait_watermark_1m", "provisional"), ("wait_watermark_15m", "finalize"),
                            ("wait_watermark_60m", "late_after_final_check")):
            s_, n_ = by.get((h, sensor)), by.get((h, nxt))
            if s_ and n_ and s_["end"] and n_["start"]:
                hand_off.append(n_["start"] - s_["end"])

    c.command("SYSTEM FLUSH LOGS")
    pokes = c.query(f"""SELECT count(), quantiles(0.5, 0.99)(query_duration_ms), max(query_duration_ms),
                               quantiles(0.5, 0.99)(read_rows)
                        FROM system.query_log WHERE type = 'QueryFinish' AND event_time_microseconds >= {metrics._ts(q_start)}
                          AND query LIKE 'SELECT uniqExact(kafka_partition) FROM events WHERE event_time >=%'"""
                    ).result_rows[0]
    batches = c.query("""SELECT count(), countIf(parts > 1) FROM (
                           SELECT inserted_at, uniqExact(_part) parts FROM events GROUP BY inserted_at)""").result_rows[0]
    cons_out = open(os.path.join(ROOT, "data", "c2_consumer.log")).read().strip().splitlines()
    inj_by_hour = {x["hour"]: x for x in injected}
    for x in injected:
        x["virtual"] = to_sim(x["wall"]).isoformat()
        got = c.query(f"""SELECT count() FROM events WHERE event_id IN
                          ({','.join("'" + i + "'" for i in x['event_ids'])})""").result_rows[0][0]
        x["ingested"] = got
    checks = {h: [x for x in late if x[0] == h] for h in range(a.hours)}
    res = {
        "speed": a.speed, "hours": a.hours, "until": until.isoformat(), "wall0": wall0, "unpause_wall": unpause_wall,
        "unpause_out": st.get("unpause_out", "").strip().splitlines()[-1:],
        "dag_runs": [{"hour": datetime.fromisoformat(r[1]).astimezone(timezone.utc).hour, "state": r[2]}
                     for r in runs],
        "per_hour": per_hour,
        "summary": {
            "v1_missing_excl_injected_total": sum(v["missing_events_excl_injected"] for r in per_hour
                                                  for v in r["versions"] if v["stage"] == "provisional"),
            "v1_hours_incomplete_excl_injected": sum(any(v["missing_events_excl_injected"] > 0 for v in r["versions"]
                                                         if v["stage"] == "provisional") for r in per_hour),
            "v2_missing_excl_injected_total": sum(v["missing_events_excl_injected"] for r in per_hour
                                                  for v in r["versions"] if v["stage"] == "final"),
            "v2_missing_total_incl_injected": sum(v["missing_events"] for r in per_hour for v in r["versions"]
                                                  if v["stage"] == "final"),
            "latest_matches_truth_hours": sum(r["latest_matches_truth"] for r in per_hour),
            "late_after_final_by_hour": {h: [x[2] for x in checks[h]] for h in range(a.hours)},
            "injected_by_hour": {h: inj_by_hour[h]["n"] for h in inj_by_hour},
            "refinalized_hours": sorted({x[0] for x in late if x[4] > 0}),
            "provisional_start_virtual_min_after_gate": [r.get("start_virtual_min_after_gate") for r in task_rows
                                                         if r["task"] == "provisional"],
            "finalize_start_virtual_min_after_gate": [r.get("start_virtual_min_after_gate") for r in task_rows
                                                      if r["task"] == "finalize"],
            "sensor_to_next_task_start_s": {"median": round(statistics.median(hand_off), 2),
                                            "max": round(max(hand_off), 2), "n": len(hand_off)} if hand_off else None,
        },
        "watermark_poke_queries": {"count": pokes[0], "duration_ms_p50_p99": pokes[1], "duration_ms_max": pokes[2],
                                   "read_rows_p50_p99": pokes[3]},
        "insert_batches": {"distinct_inserted_at": batches[0], "inserted_at_spanning_multiple_parts": batches[1],
                           "consumer_last_line": cons_out[-1] if cons_out else None},
        "injected": injected, "tasks": task_rows, "docker_stats": st["docker_stats"],
        "cpus": os.cpu_count(),
    }
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=os.path.join(ROOT, "data", "day1.tsv"))
    ap.add_argument("--speed", type=float, default=60)
    ap.add_argument("--hours", type=int, default=12)
    ap.add_argument("--inject-hours", type=int, nargs="*", default=[2, 5, 8])
    ap.add_argument("--inject-n", type=int, default=5)
    ap.add_argument("--name", default="metrics_dag_mac_m4")
    ap.add_argument("--collect-from", help="재생 없이 data/c2_raw_state.json으로 결과 수집만 다시 한다")
    a = ap.parse_args()
    if a.collect_from:
        res = collect(ch(), a, json.load(open(a.collect_from)))
        print(save(a.name, res))
        print(json.dumps(res["summary"], ensure_ascii=False, indent=1))
        raise SystemExit
    until = SIM0 + timedelta(hours=a.hours + 1, minutes=10)
    c = ch()
    reset_topics()
    create_events_table(c, "events")
    metrics.create_tables(c, drop=True)
    reset_dag()
    c.command("SYSTEM FLUSH LOGS")
    q_start = c.query("SELECT now64(3)").result_rows[0][0]
    group = f"c2-{int(time.time())}"
    cons = spawn("slp.consume", "--group", group, "--instance-id", "ingest-c2",
                 log=os.path.join(ROOT, "data", "c2_consumer.log"))
    time.sleep(3)
    wall0 = time.time() + 3
    prod = spawn("slp.produce", a.data, "--speed", str(a.speed), "--sim0", SIM0.isoformat(),
                 "--start-at", str(wall0), "--until", until.isoformat(),
                 log=os.path.join(ROOT, "data", "c2_producer.log"))
    unpause_out = af("dags", "unpause", DAG)
    unpause_wall = time.time()
    sampler = Sampler()
    sampler.start()
    injected: list[dict] = []
    inj = threading.Thread(target=injector, args=(ch(), a.inject_hours, a.inject_n, injected), daemon=True)
    inj.start()
    to_sim = lambda w: SIM0 + timedelta(seconds=(w - wall0) * a.speed)  # noqa: E731

    deadline = time.time() + (a.hours + 2) * 3600 / a.speed + 600
    while time.time() < deadline:
        states = psql(f"select state from dag_run where dag_id = '{DAG}'")
        if len(states) == a.hours and all(s[0] in ("success", "failed") for s in states):
            break
        time.sleep(5)
    prod.wait()
    time.sleep(5)
    cons.terminate()
    cons.wait()
    inj.join(timeout=1)
    sampler.stop_ev.set()
    # 수집 단계에서 실패해도 다시 계산할 수 없는 값(벽시계 기준점·주입 기록·메모리 표본)을 먼저 파일로 남긴다.
    st = {"wall0": wall0, "unpause_wall": unpause_wall, "unpause_out": unpause_out, "injected": injected,
          "docker_stats": sampler.rows, "q_start": str(q_start)}
    with open(os.path.join(ROOT, "data", "c2_raw_state.json"), "w") as f:
        json.dump(st, f)
    res = collect(c, a, st)

    print(save(a.name, res))
    print(json.dumps(res["summary"], ensure_ascii=False, indent=1))

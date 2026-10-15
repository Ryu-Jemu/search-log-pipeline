"""관측 실행(observability_mac_m4.json) 뒤의 events 표에서 적재 지연(inserted_at − kafka_ts)을 장애 구간별로 나눠 계산한다.

- 분위수는 quantileExact(결정적)로 계산한다. 처음 저장한 값은 근사 quantile이라 다시 계산할 때마다 1ms 안팎으로 달랐다.
- 구간은 Kafka 도착 시각(kafka_ts) 기준: 첫 장애 전 / 수집기 SIGTERM~재시작 / 재시작 뒤.
- 형식 오류 주입 구간의 DLQ 사유별 증가량(Prometheus)과 수집기 프로세스별 DLQ 건수(로그)도 함께 남긴다.
  Prometheus는 보존 기간(2일)이 지나면 사라지므로 값을 파일로 옮겨 둔다.
"""
import json
import os
import urllib.parse
import urllib.request

from common import ROOT, save

from slp.infra import ch


def q(c, lo: float, hi: float) -> dict:
    n, (p50, p95, p99), mx = c.query(f"""
        SELECT count(), quantilesExact(0.5, 0.95, 0.99)(dateDiff('millisecond', kafka_ts, inserted_at)),
               max(dateDiff('millisecond', kafka_ts, inserted_at))
        FROM events WHERE toUnixTimestamp64Milli(kafka_ts) / 1000 >= {lo}
                      AND toUnixTimestamp64Milli(kafka_ts) / 1000 < {hi}""").result_rows[0]
    return {"events": n, "p50_ms": p50, "p95_ms": p95, "p99_ms": p99, "max_ms": mx}


def last_json(path: str) -> dict:
    return json.loads(open(path).read().strip().splitlines()[-1])


if __name__ == "__main__":
    d = json.load(open(os.path.join(ROOT, "docs", "results", "observability_mac_m4.json")))
    ev = {e["what"]: e["t"] for e in d["events"]}
    c = ch()
    assert c.query("SELECT count() FROM events").result_rows[0][0] == d["truth"]["rows"], "events 표가 그 실행의 것이 아니다"
    fp_rows, fp_inserts = c.query(f"""SELECT count(), uniqExact(inserted_at) FROM events
        WHERE toUnixTimestamp64Milli(inserted_at) / 1000 BETWEEN {ev['failpoint_collector_start']}
                                                           AND {ev['failpoint_collector_dead']}""").result_rows[0]
    prom = json.load(urllib.request.urlopen("http://localhost:9090/api/v1/query?" + urllib.parse.urlencode(
        {"query": "sum by (reason) (increase(slp_dlq_total[1m]))", "time": ev["dlq_burst_end"]})))
    burst = {r["metric"]["reason"]: round(float(r["value"][1]), 2) for r in prom["data"]["result"]}
    logs = {n: os.path.join(ROOT, "data", f"{n}.log") for n in ("d_consumer_0", "d_consumer_failpoint", "d_consumer_1")}
    dlq_by_proc = {n: (last_json(p)["dlq"] if open(p).read().strip().splitlines()[-1].startswith("{") else None)
                   for n, p in logs.items()}
    res = {"note": ("observability_mac_m4 실행의 events 표에서 Kafka 도착 시각(kafka_ts) 구간별 적재 지연. quantileExact. "
                    "처음 저장한 값(근사 quantile)을 이 스크립트로 다시 계산해 덮어썼다."),
           "before_first_fault": q(c, d["wall0"], ev["dlq_burst_start"]),
           "arrived_during_collector_outage": q(c, ev["collector_sigterm"], ev["collector_restart"]),
           "after_restart": q(c, ev["collector_restart"], d["end"]),
           "failpoint_collector": {"rows_inserted_before_kill": fp_rows, "inserts": fp_inserts},
           "dlq_increase_1m_at_burst_end_by_reason": burst,
           "dlq_messages_by_collector_process": dlq_by_proc, "dlq_topic_total": d["truth"]["dlq_messages"],
           "collector_logs_head_tail": {n: open(p).read().strip().splitlines()[:2] + open(p).read().strip().splitlines()[-1:]
                                        for n, p in logs.items()}}
    print(json.dumps(res, ensure_ascii=False, indent=1))
    print(save("observability_latency_mac_m4", res, overwrite=True))

"""실험 C3: 시간별 집계를 원본에서 매번 계산할 때와 MV(AggregatingMergeTree)·버전 표를 읽을 때의 조회 비용을
여러 날 규모에서 다시 잰다(실험 C에서는 시간당 1~4만 건 규모라 MV가 빠르지 않았다).

- 데이터: 재생기 seed 42, 가상 7일(--hours 168). 첫날은 day1.tsv와 같지 않다(캠페인 수가 늘어 난수 사용 순서가 다르다).
- 적재: 별도 DB(slp_scale)에 MV를 먼저 만든 뒤 Kafka → 수집기 경로로 적재한다(slp.events에 MV를 붙이지 않는다).
- 조회(범위 1·3·7일):
    hour   : 한 시간 지표(검색·클릭·고유 사용자)
    series : 범위 안 시간별 지표 전체
  각각 raw(events 집계) / mv(hourly_mv uniqExactMerge) / mv_g1(같은 MV, index_granularity=1) /
  versioned(hourly_metrics FINAL, 이미 계산된 값) 네 방식. mv_g1은 1회차(mv_scale_run1_*.json)를 보고 추가했다.
- 측정: 서버 측 system.query_log(시작·종료 마이크로초 시각 차, read_rows, read_bytes). 워밍업 1회 뒤 REPS회, 네 방식을
  번갈아 실행해 중앙값·p90. 쿼리 캐시 끔. 상태 두 가지: 적재·백그라운드 병합 직후 / OPTIMIZE FINAL 뒤.
  페이지 캐시는 비우지 않는다(웜 캐시 기준).
- 네 방식의 결과가 모든 시간에서 같은지 먼저 확인한다(다르면 비교 자체가 무의미).
"""
import argparse
import os
import statistics
import subprocess
import time
import uuid
from datetime import datetime, timedelta, timezone

from common import ROOT, PY, ENV, save, spawn

from slp import metrics
from slp.infra import ch, create_events_table, reset_topics

DB = "slp_scale"
SIM0 = datetime(2026, 1, 5, tzinfo=timezone.utc)
MV_DDL = [
    """CREATE TABLE hourly_mv (hour DateTime('UTC'),
         searches SimpleAggregateFunction(sum, UInt64), clicks SimpleAggregateFunction(sum, UInt64),
         users AggregateFunction(uniqExact, String))
       ENGINE = AggregatingMergeTree ORDER BY hour""",
    """CREATE MATERIALIZED VIEW hourly_mv_view TO hourly_mv AS
       SELECT toStartOfHour(event_time) AS hour, countIf(event_type = 'search') AS searches,
              countIf(event_type = 'click') AS clicks, uniqExactState(user_id) AS users
       FROM events GROUP BY hour""",
    # 1회차에서 MV 한 시간 조회가 MV 표 전체 행(그래뉼 기본 8192행)을 읽는 것을 보고 추가한 변형:
    # 그래뉼을 1행으로 줄이면 필요한 시간의 집계 상태만 읽는지 확인한다.
    """CREATE TABLE hourly_mv_g1 (hour DateTime('UTC'),
         searches SimpleAggregateFunction(sum, UInt64), clicks SimpleAggregateFunction(sum, UInt64),
         users AggregateFunction(uniqExact, String))
       ENGINE = AggregatingMergeTree ORDER BY hour SETTINGS index_granularity = 1""",
    """CREATE MATERIALIZED VIEW hourly_mv_g1_view TO hourly_mv_g1 AS
       SELECT toStartOfHour(event_time) AS hour, countIf(event_type = 'search') AS searches,
              countIf(event_type = 'click') AS clicks, uniqExactState(user_id) AS users
       FROM events GROUP BY hour""",
]
METHODS = ("raw", "mv", "mv_g1", "versioned")


def rng(start: datetime, hours: int, col: str = "event_time") -> str:
    end = start + timedelta(hours=hours)
    return (f"{col} >= toDateTime('{start:%Y-%m-%d %H:%M:%S}', 'UTC') "
            f"AND {col} < toDateTime('{end:%Y-%m-%d %H:%M:%S}', 'UTC')")


def queries(days: int) -> dict:
    h0 = SIM0 + timedelta(days=days - 1, hours=16)  # 범위 마지막 날 16시(하루 중 최대 유입 시간)
    s0, n = SIM0, days * 24
    return {
        ("hour", "raw"): f"""SELECT countIf(event_type = 'search'), countIf(event_type = 'click'), uniqExact(user_id)
                             FROM events WHERE {rng(h0, 1)}""",
        ("hour", "mv"): f"""SELECT sum(searches), sum(clicks), uniqExactMerge(users) FROM hourly_mv
                            WHERE hour = toDateTime('{h0:%Y-%m-%d %H:%M:%S}', 'UTC')""",
        ("hour", "mv_g1"): f"""SELECT sum(searches), sum(clicks), uniqExactMerge(users) FROM hourly_mv_g1
                               WHERE hour = toDateTime('{h0:%Y-%m-%d %H:%M:%S}', 'UTC')""",
        ("hour", "versioned"): f"""SELECT searches, clicks, unique_users FROM hourly_metrics FINAL
                                   WHERE hour = toDateTime('{h0:%Y-%m-%d %H:%M:%S}', 'UTC')""",
        ("series", "raw"): f"""SELECT toStartOfHour(event_time) h, countIf(event_type = 'search'),
                                      countIf(event_type = 'click'), uniqExact(user_id)
                               FROM events WHERE {rng(s0, n)} GROUP BY h ORDER BY h""",
        ("series", "mv"): f"""SELECT hour, sum(searches), sum(clicks), uniqExactMerge(users) FROM hourly_mv
                              WHERE {rng(s0, n, 'hour')} GROUP BY hour ORDER BY hour""",
        ("series", "mv_g1"): f"""SELECT hour, sum(searches), sum(clicks), uniqExactMerge(users) FROM hourly_mv_g1
                                 WHERE {rng(s0, n, 'hour')} GROUP BY hour ORDER BY hour""",
        ("series", "versioned"): f"""SELECT hour, searches, clicks, unique_users FROM hourly_metrics FINAL
                                     WHERE {rng(s0, n, 'hour')} ORDER BY hour""",
    }


def wait_merges(c, timeout=300):
    t = time.time()
    while time.time() - t < timeout:
        if c.query(f"SELECT count() FROM system.merges WHERE database = '{DB}'").result_rows[0][0] == 0:
            return round(time.time() - t, 1)
        time.sleep(1)
    return None


def table_stats(c) -> dict:
    return {t: dict(zip(["rows", "parts", "bytes_on_disk"], r)) for t, *r in c.query(f"""
        SELECT table, sum(rows), count(), sum(bytes_on_disk) FROM system.parts
        WHERE database = '{DB}' AND active GROUP BY table""").result_rows}


def measure(c, state: str, reps: int, run_tag: str) -> list[dict]:
    plan = []
    for days in (1, 3, 7):
        for shape in ("hour", "series"):
            qs = queries(days)
            for i in range(reps + 1):  # 0번째는 워밍업
                for method in METHODS:  # 번갈아 실행
                    qid = f"{run_tag}-{state}-{days}-{shape}-{method}-{i}-{uuid.uuid4().hex[:6]}"
                    t = time.perf_counter()
                    c.query(qs[(shape, method)], settings={"use_query_cache": 0, "query_id": qid})
                    plan.append({"days": days, "shape": shape, "method": method, "rep": i, "query_id": qid,
                                 "client_ms": (time.perf_counter() - t) * 1000})
    c.command("SYSTEM FLUSH LOGS")
    logs = {r[0]: r[1:] for r in c.query(f"""SELECT query_id,
            dateDiff('microsecond', query_start_time_microseconds, event_time_microseconds) / 1000, read_rows, read_bytes
        FROM system.query_log WHERE type = 'QueryFinish' AND query_id LIKE '{run_tag}-{state}-%'""").result_rows}
    out = []
    for days in (1, 3, 7):
        for shape in ("hour", "series"):
            for method in METHODS:
                rows = [p for p in plan if (p["days"], p["shape"], p["method"]) == (days, shape, method)
                        and p["rep"] > 0]
                dur = sorted(logs[p["query_id"]][0] for p in rows)
                out.append({"state": state, "days": days, "shape": shape, "method": method, "n": len(dur),
                            "server_ms_median": round(statistics.median(dur), 3),
                            "server_ms_p90": round(dur[int(0.9 * (len(dur) - 1))], 3),
                            "read_rows": logs[rows[0]["query_id"]][1], "read_bytes": logs[rows[0]["query_id"]][2],
                            "client_ms_median": round(statistics.median(p["client_ms"] for p in rows), 2)})
    return out


def results_equal(c) -> dict:
    """네 방식이 7일 168시간 모두 같은 값을 내는지."""
    qs = queries(7)
    def rows(q):
        return [(str(r[0]),) + tuple(int(x) for x in r[1:]) for r in c.query(q).result_rows]
    raw, mv, ver = rows(qs[("series", "raw")]), rows(qs[("series", "mv")]), rows(qs[("series", "versioned")])
    g1 = rows(qs[("series", "mv_g1")])
    return {"hours": len(raw), "raw_eq_mv": raw == mv, "raw_eq_mv_g1": raw == g1, "raw_eq_versioned": raw == ver}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=os.path.join(ROOT, "data", "7d_s42.tsv"))
    ap.add_argument("--reps", type=int, default=20)
    ap.add_argument("--name", default="mv_scale_mac_m4")
    ap.add_argument("--keep", action="store_true", help="끝난 뒤 slp_scale DB를 지우지 않는다")
    a = ap.parse_args()
    if not os.path.exists(a.data):
        gen = subprocess.run([PY, "-m", "slp.generator", "--hours", "168", "--seed", "42",
                              "--labels", os.path.join(ROOT, "data", "labels_7d_s42.json")],
                             env=ENV, cwd=ROOT, stdout=open(a.data, "wb"), stderr=subprocess.PIPE, check=True)
        print(gen.stderr.decode().strip())
    admin = ch()
    admin.command(f"DROP DATABASE IF EXISTS {DB} SYNC")
    admin.command(f"CREATE DATABASE {DB}")
    c = ch(database=DB)
    create_events_table(c, "events")
    for s in MV_DDL:
        c.command(s)
    metrics.create_tables(c)
    reset_topics()
    t0 = time.time()
    spawn("slp.produce", a.data, log=os.path.join(ROOT, "data", "c3_producer.log")).wait()
    spawn("slp.consume", "--group", f"c3-{int(t0)}", "--instance-id", "ingest-c3", "--idle-exit", "5",
          log=os.path.join(ROOT, "data", "c3_consumer.log"), env={"CH_DATABASE": DB}).wait()
    load_s = round(time.time() - t0, 1)
    merge_wait_s = wait_merges(c)
    # 버전 표: DAG와 같은 함수로 168시간 확정값을 계산해 둔다(조회 비교용).
    t1 = time.time()
    for h in range(168):
        metrics.compute_hour(c, SIM0 + timedelta(hours=h), 2, "final", 1)
    versioned_build_s = round(time.time() - t1, 1)
    wait_merges(c)
    stats_load = table_stats(c)
    eq = results_equal(c)
    tag = f"c3{int(time.time())}"
    m1 = measure(c, "after_load", a.reps, tag)
    for t in ("events", "hourly_mv", "hourly_mv_g1", "hourly_metrics"):
        c.command(f"OPTIMIZE TABLE {t} FINAL")
    wait_merges(c)
    stats_opt = table_stats(c)
    m2 = measure(c, "after_optimize", a.reps, tag)
    res = {"data": os.path.basename(a.data), "rows_valid": stats_load.get("events", {}).get("rows"),
           "load_s_incl_5s_idle_exit": load_s, "merge_wait_s": merge_wait_s, "versioned_build_s": versioned_build_s,
           "results_equal": eq, "tables_after_load": stats_load, "tables_after_optimize": stats_opt,
           "measurements": m1 + m2, "reps": a.reps, "cpus": os.cpu_count(),
           "running_services": subprocess.run(["docker", "compose", "-f", os.path.join(ROOT, "docker-compose.yml"),
                                               "ps", "--format", "{{.Service}}"], capture_output=True,
                                              text=True).stdout.split(),
           "note": "서버 측 소요 시간(query_log의 시작·종료 마이크로초 시각 차) 중앙값·p90, 워밍업 1회 제외, "
                   "쿼리 캐시 끔, 웜 페이지 캐시"}
    print(save(a.name, res))
    for r in m1 + m2:
        print(r["state"], r["days"], r["shape"], r["method"], r["server_ms_median"], r["read_rows"])
    print(eq)
    if not a.keep:
        admin.command(f"DROP DATABASE IF EXISTS {DB} SYNC")

"""실험 C: 늦게 도착한 이벤트 때문에 시간별 지표가 덜 집계되는 문제와 대안 비교.

day1을 가상 시각 SPEED배속으로 재생한다(기본 60배: 가상 1시간 = 실제 1분).
같은 스트림 위에서 각 시간 H가 끝난 뒤 D분(가상)에 다음을 동시에 계산해 기록한다.
  raw : 원본 이벤트 테이블을 그 시점에 집계 (D=0이 '정시 1회 집계' 기준선, D>0은 '허용 지연 대기')
  mv  : MV로 증분 집계된 표를 그 시점에 조회
  ver : D ∈ {0, 15, 60}에 버전을 올려 재집계 (ReplacingMergeTree(version))
끝난 뒤 최종 적재본을 정답으로 대조한다.
"""
import argparse
import os
import statistics
import time
from datetime import datetime, timedelta, timezone

from common import ROOT, save, spawn

from slp.infra import ch, create_events_table, reset_topics

DELAYS = [0, 1, 2, 5, 10, 15, 30, 60]  # 가상 분
VERSIONS = {0: 1, 15: 2, 60: 3}

SETUP = [
    """CREATE TABLE hourly_mv (hour DateTime('UTC'),
         searches SimpleAggregateFunction(sum, UInt64), clicks SimpleAggregateFunction(sum, UInt64),
         users AggregateFunction(uniqExact, String))
       ENGINE = AggregatingMergeTree ORDER BY hour""",
    """CREATE MATERIALIZED VIEW hourly_mv_view TO hourly_mv AS
       SELECT toStartOfHour(event_time) AS hour, countIf(event_type = 'search') AS searches,
              countIf(event_type = 'click') AS clicks, uniqExactState(user_id) AS users
       FROM events GROUP BY hour""",
    """CREATE TABLE hourly_snapshots (strategy LowCardinality(String), hour DateTime('UTC'), delay_min UInt16,
         searches UInt64, clicks UInt64, users UInt64, query_ms Float64, due_wall Float64, exec_wall Float64)
       ENGINE = MergeTree ORDER BY (strategy, hour, delay_min)""",
    """CREATE TABLE hourly_versioned (hour DateTime('UTC'), version UInt32, searches UInt64, clicks UInt64,
         users UInt64, computed_at DateTime64(3, 'UTC')) ENGINE = ReplacingMergeTree(version) ORDER BY hour""",
]
RAW = """select countIf(event_type = 'search'), countIf(event_type = 'click'), uniqExact(user_id)
         from events where event_time >= toDateTime('{h}', 'UTC') and event_time < toDateTime('{h}', 'UTC') + interval 1 hour"""
MV = """select sum(searches), sum(clicks), uniqExactMerge(users) from hourly_mv where hour = toDateTime('{h}', 'UTC')"""


def q(c, sql):
    t = time.perf_counter()
    r = c.query(sql).result_rows[0]
    return [int(x or 0) for x in r], (time.perf_counter() - t) * 1000


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=os.path.join(ROOT, "data", "day1.tsv"))
    ap.add_argument("--speed", type=float, default=60)
    ap.add_argument("--hours", type=int, default=12)
    a = ap.parse_args()
    sim0 = datetime(2026, 1, 5, tzinfo=timezone.utc)
    until = sim0 + timedelta(hours=a.hours, minutes=20)  # 마지막 시간의 늦은 이벤트(최대 10분)까지 보낸다
    c = ch()
    for t in ("hourly_mv_view", "hourly_mv", "hourly_snapshots", "hourly_versioned"):
        c.command(f"DROP TABLE IF EXISTS {t} SYNC")
    reset_topics()
    create_events_table(c, "events")
    for s in SETUP:
        c.command(s)
    group = f"c-{int(time.time())}"
    cons = spawn("slp.consume", "--group", group, "--instance-id", "ingest-c",
                 log=os.path.join(ROOT, "data", "c_consumer.log"))
    time.sleep(3)
    wall0 = time.time() + 2
    prod = spawn("slp.produce", a.data, "--speed", str(a.speed), "--sim0", sim0.isoformat(),
                 "--start-at", str(wall0), "--until", until.isoformat(),
                 log=os.path.join(ROOT, "data", "c_producer.log"))
    to_wall = lambda sim: wall0 + (sim - sim0).total_seconds() / a.speed  # noqa: E731
    jobs = sorted((to_wall(sim0 + timedelta(hours=h + 1, minutes=d)), h, d)
                  for h in range(a.hours) for d in DELAYS)
    lag_ms = []
    for due, h, d in jobs:
        time.sleep(max(0.0, due - time.time()))
        lag_ms.append((time.time() - due) * 1000)
        hs = (sim0 + timedelta(hours=h)).strftime("%Y-%m-%d %H:%M:%S")
        for strategy, sql in (("raw", RAW), ("mv", MV)):
            ex = time.time()  # 같은 시각에 예정된 작업이 여러 개면 실제 실행은 조금씩 밀린다 — 기록해 둔다
            (s, k, u), ms = q(c, sql.format(h=hs))
            c.insert("hourly_snapshots", [[strategy, sim0 + timedelta(hours=h), d, s, k, u, ms, due, ex]],
                     column_names=["strategy", "hour", "delay_min", "searches", "clicks", "users", "query_ms",
                                   "due_wall", "exec_wall"])
        if d in VERSIONS:
            (s, k, u), _ = q(c, RAW.format(h=hs))
            c.insert("hourly_versioned", [[sim0 + timedelta(hours=h), VERSIONS[d], s, k, u, datetime.now(timezone.utc)]],
                     column_names=["hour", "version", "searches", "clicks", "users", "computed_at"])
    prod.wait()
    time.sleep(5)
    cons.terminate()
    cons.wait()

    # 정답: 재생이 끝난 뒤 최종 적재본
    final = {}
    for h in range(a.hours):
        hs = (sim0 + timedelta(hours=h)).strftime("%Y-%m-%d %H:%M:%S")
        final[h] = q(c, RAW.format(h=hs))[0]
    snaps = c.query("""select strategy, toHour(hour), delay_min, searches, clicks, users, query_ms
                       from hourly_snapshots order by strategy, hour, delay_min""").result_rows
    table = {}
    for strategy, h, d, s, k, u, ms in snaps:
        fs, fk, fu = final[h]
        table.setdefault(strategy, {}).setdefault(d, []).append(
            {"hour": h, "missing_events": (fs + fk) - (s + k), "missing_users": fu - u,
             "complete_pct": round(100 * (s + k) / (fs + fk), 3), "query_ms": round(ms, 1)})
    summary = {}
    for strategy, by_d in table.items():
        for d, rows in sorted(by_d.items()):
            summary[f"{strategy}@+{d}m"] = {
                "missing_events_total": sum(r["missing_events"] for r in rows),
                "hours_incomplete": sum(r["missing_events"] > 0 for r in rows),
                "min_complete_pct": min(r["complete_pct"] for r in rows),
                "median_query_ms": round(statistics.median(r["query_ms"] for r in rows), 1)}
    # 기준선(raw, +0분)에서 빠진 이벤트를 원인별로 나눈다.
    #   late      : 시간이 마감된 뒤에야 Kafka에 들어온 이벤트(늦은 도착)
    #   in_flight : 마감 전에 들어왔지만 집계 쿼리 실행 시점까지 아직 적재되지 않은 이벤트(파이프라인 처리 중)
    split_rows = c.query(f"""
        select toHour(e.event_time) h,
               countIf(toUnixTimestamp64Milli(e.kafka_ts) / 1000 > s.due_wall) late,
               countIf(toUnixTimestamp64Milli(e.kafka_ts) / 1000 <= s.due_wall
                       and toUnixTimestamp64Milli(e.inserted_at) / 1000 > s.exec_wall) in_flight,
               any(s.exec_wall - s.due_wall) * 1000 exec_delay_ms
        from events e join (select hour, due_wall, exec_wall from hourly_snapshots
                            where strategy = 'raw' and delay_min = 0) s on toStartOfHour(e.event_time) = s.hour
        group by h order by h""").result_rows
    split = (sum(r[1] for r in split_rows), sum(r[2] for r in split_rows))
    versions = c.query("""select toHour(hour), version, searches + clicks from hourly_versioned final order by hour""").result_rows
    ver_ok = all(v[2] == sum(final[v[0]][:2]) for v in versions)
    mv_ms = [round(q(c, MV.format(h=(sim0 + timedelta(hours=h)).strftime('%Y-%m-%d %H:%M:%S')))[1], 1) for h in range(a.hours)]
    raw_ms = [round(q(c, RAW.format(h=(sim0 + timedelta(hours=h)).strftime('%Y-%m-%d %H:%M:%S')))[1], 1) for h in range(a.hours)]
    res = {"speed": a.speed, "hours": a.hours, "final_events_per_hour": {h: v[0] + v[1] for h, v in final.items()},
           "summary": summary, "wall0": wall0,
           "baseline_missing_split": {"late_after_hour_close": split[0], "in_flight_at_query": split[1],
                                      "per_hour": [list(r) for r in split_rows]},
           "versioned_final_matches_truth": ver_ok, "versioned_final": versions,
           "scheduler_delay_ms_median": round(statistics.median(lag_ms), 1),
           "end_query_ms_median": {"raw": statistics.median(raw_ms), "mv": statistics.median(mv_ms)},
           "detail": table, "cpus": os.cpu_count()}
    save("c_late_events", res)
    for k, v in summary.items():
        print(k, v)
    print({k: v for k, v in res.items() if k not in ("detail", "summary", "versioned_final", "final_events_per_hour")})

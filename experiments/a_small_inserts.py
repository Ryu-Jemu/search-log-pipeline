"""실험 A: 작은 단위 INSERT가 처리량·파트 수·lag에 주는 영향과 대안 비교.

모든 변형은 같은 입력(day1.tsv 앞 N줄)을 같은 속도(R건/초)로 Kafka에 넣고,
생산 종료 후 DRAIN초까지 기다린 뒤 결과를 잰다.
"""
import argparse
import os
import signal
import time

from common import ROOT, Sampler, latency, save, spawn, truth

from slp.infra import ch, create_events_table, reset_topics

KAFKA_ENGINE_SQL = [
    """CREATE TABLE kafka_src (raw String) ENGINE = Kafka SETTINGS
         kafka_broker_list = 'kafka:19092', kafka_topic_list = 'search-events',
         kafka_group_name = '{group}', kafka_format = 'RawBLOB', kafka_num_consumers = 1{extra}""",
    """CREATE TABLE dlq_engine (raw String, reason LowCardinality(String), src String,
         at DateTime64(3, 'UTC') DEFAULT now64(3)) ENGINE = MergeTree ORDER BY at""",
    # 파이썬 validate.parse와 같은 규칙을 SQL로 옮겼다.
    """CREATE MATERIALIZED VIEW mv_events TO {table} AS
       WITH isValidJSON(raw) AS ok_json,
            JSONExtractString(raw, 'event_type') AS et,
            JSONExtractString(raw, 'user_id') AS uid,
            parseDateTime64BestEffortOrNull(JSONExtractString(raw, 'event_time'), 3, 'UTC') AS ts,
            lowerUTF8(trimBoth(replaceRegexpAll(JSONExtractString(raw, 'query'), '\\\\s+', ' '))) AS qn,
            JSONHas(raw, 'doc_id') AS has_doc
       SELECT toUUID(JSONExtractString(raw, 'event_id')) AS event_id, et AS event_type,
              assumeNotNull(ts) AS event_time, uid AS user_id,
              JSONExtractString(raw, 'session_id') AS session_id, JSONExtractUInt(raw, 'qid') AS qid,
              JSONExtractString(raw, 'query') AS query, qn AS query_norm,
              if(has_doc, toNullable(toUInt32(JSONExtractUInt(raw, 'doc_id'))), NULL) AS doc_id,
              assumeNotNull(_timestamp_ms) AS kafka_ts, _partition AS kafka_partition, _offset AS kafka_offset,
              -- now64()는 스트리밍 쿼리 시작 시각으로 고정돼 지연이 음수로 나온다(실측). 블록 단위 시각을 쓴다(초 단위).
              toDateTime64(nowInBlock('UTC'), 3) AS inserted_at
       FROM kafka_src
       WHERE ok_json AND et IN ('search', 'click') AND uid != '' AND ts IS NOT NULL
             AND (et = 'search' OR has_doc) AND qn != ''""",
    """CREATE MATERIALIZED VIEW mv_dlq TO dlq_engine AS
       WITH isValidJSON(raw) AS ok_json,
            JSONExtractString(raw, 'event_type') AS et,
            JSONExtractString(raw, 'user_id') AS uid,
            parseDateTime64BestEffortOrNull(JSONExtractString(raw, 'event_time'), 3, 'UTC') AS ts,
            lowerUTF8(trimBoth(replaceRegexpAll(JSONExtractString(raw, 'query'), '\\\\s+', ' '))) AS qn,
            JSONHas(raw, 'doc_id') AS has_doc
       SELECT raw, multiIf(NOT ok_json, 'bad_json', et NOT IN ('search', 'click'), 'bad_event_type',
                           uid = '', 'bad_user_id', ts IS NULL, 'bad_value',
                           et = 'click' AND NOT has_doc, 'missing_doc_id', qn = '', 'empty_query', 'ok') AS reason,
              concat(toString(_partition), ':', toString(_offset)) AS src
       FROM kafka_src WHERE reason != 'ok'""",
]


def drop_engine(c):
    for t in ("mv_events", "mv_dlq", "kafka_src", "dlq_engine"):
        c.command(f"DROP TABLE IF EXISTS {t} SYNC")


def run_variant(name, consumers, consume_args, data, n, rate, drain, expected):
    c = ch()
    drop_engine(c)
    reset_topics()
    create_events_table(c, "events")
    group = f"a-{name}-{int(time.time())}"
    procs = []
    if consumers == "engine":
        for sql in KAFKA_ENGINE_SQL:
            c.command(sql.format(group=group, table="events", extra="".join(", " + x for x in consume_args)))
    else:
        for i in range(consumers):
            procs.append(spawn("slp.consume", "--table", "events", "--group", group, *consume_args,
                               log=os.path.join(ROOT, "data", f"a_{name}_{i}.log")))
    time.sleep(3)  # 소비자 그룹 합류 대기
    s = Sampler("events", group)
    s.start()
    t0 = time.time()
    prod = spawn("slp.produce", data, "--rate", str(rate), "--limit", str(n))
    prod.wait()
    t_prod = time.time() - t0
    deadline = time.time() + drain
    while time.time() < deadline:
        if c.query("select count() from events").result_rows[0][0] >= expected:
            break
        time.sleep(1)
    t_end = time.time() - t0
    samples = s.stop()
    for p in procs:
        p.send_signal(signal.SIGTERM)
    for p in procs:
        try:
            p.wait(30)
        except Exception:  # noqa: BLE001
            p.kill()
    errors = {}
    for i in range(len(procs)):
        for line in open(os.path.join(ROOT, "data", f"a_{name}_{i}.log")):
            if line.startswith("{\"consumed\""):
                import json
                for k, v in json.loads(line)["insert_errors"].items():
                    errors[k] = errors.get(k, 0) + v
    rows = c.query("select count(), uniqExact(event_id) from events").result_rows[0]
    at_prod_end = max((x for x in samples if x["t"] <= t_prod), key=lambda x: x["t"], default=samples[0])
    res = {
        "variant": name, "consumers": consumers, "args": consume_args,
        "produce_seconds": round(t_prod, 1), "end_seconds": round(t_end, 1),
        "expected_rows": expected, "rows": rows[0], "unique_event_ids": rows[1],
        "rows_at_produce_end": at_prod_end["rows"], "lag_at_produce_end": at_prod_end.get("lag"),
        "drained": rows[1] >= expected,
        "max_active_parts": max(x["active_parts"] for x in samples),
        "insert_errors": errors,
        "throughput_rows_per_s_during_produce": round(at_prod_end["rows"] / max(at_prod_end["t"], 1)),
        "latency": latency("events") if rows[0] else None,
        "samples": samples,
    }
    if consumers == "engine":
        res["engine_dlq"] = c.query("select reason, count() from dlq_engine group by reason").result_rows
        drop_engine(c)
    return res


VARIANTS = {
    "a0_baseline_per_message": (1, ["--batch-size", "1"]),
    "a1_scaleout_6x_per_message": (6, ["--batch-size", "1"]),
    "a2_batch_5000_1s": (1, ["--batch-size", "5000", "--batch-ms", "1000"]),
    "a3_async_insert_per_message": (1, ["--batch-size", "1", "--settings",
                                        '{"async_insert": 1, "wait_for_async_insert": 1}']),
    "a4_kafka_engine_mv": ("engine", []),
    # 추가 관찰: async_insert의 응답 대기 여부, 배치 크기·주기에 따른 지연/처리량 교환
    "a3b_async_insert_nowait": (1, ["--batch-size", "1", "--settings",
                                    '{"async_insert": 1, "wait_for_async_insert": 0}']),
    "a2b_batch_100": (1, ["--batch-size", "100", "--batch-ms", "1000"]),
    "a2c_batch_flush_200ms": (1, ["--batch-size", "100000", "--batch-ms", "200"]),
    "a4b_kafka_engine_flush_500ms": ("engine", ["kafka_flush_interval_ms = 500"]),
    "a2d_batch_flush_5s": (1, ["--batch-size", "100000", "--batch-ms", "5000"]),
}

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=os.path.join(ROOT, "data", "day1.tsv"))
    ap.add_argument("--n", type=int, default=300_000)
    ap.add_argument("--rate", type=float, default=5000)
    ap.add_argument("--drain", type=float, default=60)
    ap.add_argument("--only", nargs="*")
    a = ap.parse_args()
    tr = truth(a.data, a.n)
    print("truth", {k: v for k, v in tr.items() if k != "ids"}, flush=True)
    for name, (cons, args) in VARIANTS.items():
        if a.only and name not in a.only:
            continue
        r = run_variant(name, cons, args, a.data, a.n, a.rate, a.drain, tr["valid"])
        r["truth"] = {k: v for k, v in tr.items() if k != "ids"}
        r["env"] = {"cpus": os.cpu_count(), "rate": a.rate, "n": a.n}
        save(name, r)
        print({k: v for k, v in r.items() if k != "samples"}, flush=True)

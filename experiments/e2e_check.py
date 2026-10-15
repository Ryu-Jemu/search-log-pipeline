"""종단 점검: day1 전체를 Kafka에 넣고 수집기로 적재한 뒤 정답과 1건 단위로 대조한다.

새 환경에서 재생기·수집기가 같은 결과를 내는지 확인하는 용도(README 실행 절차와 같은 경로).
"""
import argparse
import os
import time

from common import ROOT, save, spawn, truth

from confluent_kafka import Consumer, TopicPartition
from slp.infra import BOOTSTRAP, DLQ, TOPIC, ch, create_events_table, reset_topics

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=os.path.join(ROOT, "data", "day1.tsv"))
    ap.add_argument("--name", default="e2e_mac_m4")
    a = ap.parse_args()
    tr = truth(a.data)
    c = ch()
    reset_topics()
    create_events_table(c, "events")
    t0 = time.time()
    spawn("slp.produce", a.data, log=os.path.join(ROOT, "data", "e2e_producer.log")).wait()
    t1 = time.time()
    spawn("slp.consume", "--instance-id", "ingest-0", "--group", f"e2e-{int(t1)}", "--idle-exit", "5",
          log=os.path.join(ROOT, "data", "e2e_consumer.log")).wait()
    t2 = time.time()
    got = {str(r[0]) for r in c.query("select event_id from events").result_rows}
    rows, uniq, next_day = c.query(
        "select count(), uniqExact(event_id), countIf(toDate(event_time) = '2026-01-06') from events").result_rows[0]
    k = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "e2e-count"})
    dlq = k.get_watermark_offsets(TopicPartition(DLQ, 0), timeout=5)
    src = sum(h - l for l, h in (k.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=5) for p in range(6)))
    k.close()
    res = {"truth_lines": tr["lines"], "truth_valid": tr["valid"], "truth_rejected": tr["rejected"],
           "kafka_messages": src, "rows": rows, "unique": uniq, "lost": len(tr["ids"] - got),
           "unknown_ids": len(got - tr["ids"]), "duplicates": rows - uniq, "dlq_messages": dlq[1] - dlq[0],
           "events_on_2026_01_06": next_day, "produce_s": round(t1 - t0, 1),
           "consume_s_incl_5s_idle_exit": round(t2 - t1, 1),
           "consumer_log_tail": open(os.path.join(ROOT, "data", "e2e_consumer.log")).read().strip().splitlines()[-1]}
    res["pass"] = (res["rows"] == res["unique"] == tr["valid"] and res["lost"] == 0
                   and res["dlq_messages"] == tr["rejected"])
    print(res)
    print(save(a.name, res))

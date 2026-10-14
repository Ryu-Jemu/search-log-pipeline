"""Kafka → 검증·정규화 → ClickHouse 적재, 실패 이벤트는 DLQ.

실험을 위해 적재 단위·커밋 시점·INSERT 설정을 인자로 바꿀 수 있다.
  --commit auto  : 오프셋 자동 커밋(poll 시점에 저장, 1초마다 커밋) — 실험 B 기준선
  --commit after : ClickHouse 적재·DLQ 전송이 끝난 뒤 동기 커밋
"""
from __future__ import annotations

import argparse
import json
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer

from slp.infra import BOOTSTRAP, DLQ, TOPIC, ch
from slp.validate import COLUMNS, parse

INSERT_COLUMNS = COLUMNS + ["kafka_ts", "kafka_partition", "kafka_offset"]


def run(table: str, group: str, batch_size: int, batch_ms: int, commit: str,
        settings: dict, dedup_token: bool, idle_exit: float, stats_path: str | None) -> dict:
    client = ch()
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP, "group.id": group, "auto.offset.reset": "earliest",
        "enable.auto.commit": commit == "auto", "auto.commit.interval.ms": 1000,
    })
    dlq = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 50})
    c.subscribe([TOPIC])
    st = {"consumed": 0, "inserted": 0, "dlq": 0, "inserts": 0, "insert_ms": 0.0,
          "insert_errors": {}, "dlq_reasons": {}}
    rows: list[list] = []
    ranges: dict[int, list[int]] = {}
    stop = False

    def on_term(*_):
        nonlocal stop
        stop = True
    signal.signal(signal.SIGTERM, on_term)

    def flush():
        if rows:
            s = dict(settings)
            if dedup_token:
                s["insert_deduplication_token"] = ";".join(
                    f"{p}:{lo}-{hi}" for p, (lo, hi) in sorted(ranges.items()))
            while True:  # 실패하면 같은 배치를 다시 넣는다(기준선 수집기의 단순한 재시도)
                t = time.perf_counter()
                try:
                    client.insert(table, rows, column_names=INSERT_COLUMNS, settings=s)
                    break
                except Exception as e:  # noqa: BLE001 - 오류 유형을 세기 위해 전부 잡는다
                    code = type(e).__name__ + ":" + (str(e).split("Code: ")[1].split(".")[0] if "Code: " in str(e) else "?")
                    st["insert_errors"][code] = st["insert_errors"].get(code, 0) + 1
                    time.sleep(1)
                finally:
                    st["insert_ms"] += (time.perf_counter() - t) * 1000
            st["inserts"] += 1
            st["inserted"] += len(rows)
        dlq.flush()
        if commit == "after" and ranges:
            c.commit(asynchronous=False)
        rows.clear()
        ranges.clear()

    last_flush = last_msg = last_stat = time.time()
    sf = open(stats_path, "a") if stats_path else None
    while not stop:
        msg = c.poll(0.1)
        now = time.time()
        if msg is not None and not msg.error():
            last_msg = now
            st["consumed"] += 1
            p, o = msg.partition(), msg.offset()
            r = ranges.setdefault(p, [o, o])
            r[1] = o
            row, err = parse(msg.value())
            if err:
                st["dlq"] += 1
                st["dlq_reasons"][err] = st["dlq_reasons"].get(err, 0) + 1
                dlq.produce(DLQ, value=msg.value(), key=msg.key(),
                            headers={"reason": err, "src": f"{p}:{o}"})
            else:
                kts = datetime.fromtimestamp(msg.timestamp()[1] / 1000, timezone.utc)
                rows.append(row + [kts, p, o])
        if len(rows) >= batch_size or (now - last_flush) * 1000 >= batch_ms:
            flush()
            last_flush = now
        if sf and now - last_stat >= 1:
            sf.write(json.dumps({"t": round(now, 3), **{k: v for k, v in st.items() if not isinstance(v, dict)}}) + "\n")
            sf.flush()
            last_stat = now
        if idle_exit and now - last_msg > idle_exit:
            break
    flush()
    c.close()
    st["insert_ms"] = round(st["insert_ms"])
    return st


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", default="events")
    ap.add_argument("--group", default="slp-ingest")
    ap.add_argument("--batch-size", type=int, default=50000)  # 실험 A 결과로 결정
    ap.add_argument("--batch-ms", type=int, default=200)
    ap.add_argument("--commit", choices=["auto", "after"], default="after")
    ap.add_argument("--settings", default="{}", help='INSERT 설정 JSON, 예: {"async_insert": 1}')
    ap.add_argument("--dedup-token", action="store_true")
    ap.add_argument("--idle-exit", type=float, default=0)
    ap.add_argument("--stats")
    a = ap.parse_args()
    print(json.dumps(run(a.table, a.group, a.batch_size, a.batch_ms, a.commit,
                         json.loads(a.settings), a.dedup_token, a.idle_exit, a.stats)))

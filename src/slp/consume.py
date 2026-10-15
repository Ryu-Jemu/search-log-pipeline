"""Kafka → 검증·정규화 → ClickHouse 적재, 실패 이벤트는 DLQ.

실험을 위해 적재 단위·커밋 시점·INSERT 설정을 인자로 바꿀 수 있다.
  --commit auto  : 오프셋 자동 커밋(poll 시점에 저장, 1초마다 커밋) — 실험 B 기준선
  --commit after : ClickHouse 적재·DLQ 전송이 끝난 뒤 동기 커밋
  --resume-from-table : 재시작 위치를 Kafka 커밋 대신 ClickHouse에 적재된 최대 오프셋+1로 정한다
                        (데이터와 위치가 같은 INSERT로 기록되므로 재시작 시 중복·유실이 없다 — 실험 B)
  --metrics-port N    : Prometheus 지표를 N번 포트로 노출한다(기본 0 = 끔, 4단계 관측)
"""
from __future__ import annotations

import argparse
import json
import os
import random
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer

from slp.infra import BOOTSTRAP, DLQ, TOPIC, ch
from slp.validate import COLUMNS, parse

INSERT_COLUMNS = COLUMNS + ["kafka_ts", "kafka_partition", "kafka_offset"]


class Metrics:
    """Prometheus 지표. 메시지마다가 아니라 배치(flush)마다 갱신해 처리량에 주는 영향을 줄인다.
    lag는 여기서 재지 않는다(수집기가 죽으면 지표도 사라지므로 kafka-exporter가 밖에서 잰다)."""

    def __init__(self, port: int, addr: str):
        from prometheus_client import Counter, Gauge, Histogram, start_http_server
        self.consumed = Counter("slp_consumed", "Kafka에서 읽은 메시지 수")
        self.inserted = Counter("slp_inserted_rows", "ClickHouse에 적재한 행 수")
        self.dlq = Counter("slp_dlq", "DLQ로 보낸 메시지 수", ["reason"])
        self.errors = Counter("slp_insert_errors", "INSERT 실패 횟수(같은 배치 재시도 전)", ["code"])
        self.insert_s = Histogram("slp_insert_seconds", "배치 INSERT 소요 시간(재시도 포함)",
                                  buckets=(.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10))
        self.batch = Histogram("slp_batch_rows", "배치 크기(행)", buckets=(1, 10, 100, 500, 1000, 5000, 10000, 50000))
        self.last_insert = Gauge("slp_last_insert_timestamp_seconds", "마지막 INSERT 완료 시각(유닉스 초)")
        self.seen = {"consumed": 0, "dlq": {}, "errors": {}}
        start_http_server(port, addr)

    def update(self, st: dict, rows: int = 0, insert_s: float | None = None) -> None:
        self.consumed.inc(st["consumed"] - self.seen["consumed"])
        self.seen["consumed"] = st["consumed"]
        for key, metric in (("dlq_reasons", self.dlq), ("insert_errors", self.errors)):
            seen = self.seen["dlq" if key == "dlq_reasons" else "errors"]
            for k, v in st[key].items():
                metric.labels(k).inc(v - seen.get(k, 0))
                seen[k] = v
        if insert_s is not None:
            self.inserted.inc(rows)
            self.insert_s.observe(insert_s)
            self.batch.observe(rows)
            self.last_insert.set_to_current_time()


def run(table: str, group: str, batch_size: int, batch_ms: int, commit: str,
        settings: dict, dedup_token: bool, idle_exit: float, stats_path: str | None,
        resume_from_table: bool = True, instance_id: str | None = None,
        session_timeout_ms: int = 45000, failpoint_prob: float = 0.0,
        failpoint_where: str = "both", metrics_port: int = 0, metrics_addr: str = "127.0.0.1") -> dict:
    client = ch()
    prom = Metrics(metrics_port, metrics_addr) if metrics_port else None
    conf = {
        "bootstrap.servers": BOOTSTRAP, "group.id": group, "auto.offset.reset": "earliest",
        "enable.auto.commit": commit == "auto", "auto.commit.interval.ms": 1000,
        "session.timeout.ms": session_timeout_ms,
    }
    if instance_id:  # 정적 멤버십: 같은 ID로 재시작하면 리밸런스 없이 파티션을 그대로 받는다
        conf["group.instance.id"] = instance_id
    c = Consumer(conf)
    t_start = time.time()
    clock = {"last_msg": time.time(), "first_msg": None, "assigned": False}
    dlq = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 50})
    def on_assign(consumer, parts):
        if resume_from_table:
            done = dict(client.query(
                f"select kafka_partition, max(kafka_offset) from {table} group by kafka_partition").result_rows)
            for tp in parts:
                if tp.partition in done:
                    tp.offset = done[tp.partition] + 1
        consumer.assign(parts)
        clock["last_msg"] = time.time()  # 유휴 판정은 파티션을 배정받은 뒤부터 센다
        clock["assigned"] = True
        print(f"ASSIGNED {time.time() - t_start:.2f}s", flush=True)

    c.subscribe([TOPIC], on_assign=on_assign)
    st = {"consumed": 0, "inserted": 0, "dlq": 0, "inserts": 0, "insert_ms": 0.0,
          "insert_errors": {}, "dlq_reasons": {}}
    rows: list[list] = []
    ranges: dict[int, list[int]] = {}
    stop = False

    def on_term(*_):
        nonlocal stop
        stop = True
    signal.signal(signal.SIGTERM, on_term)

    def failpoint(where: str):
        # 장애 주입: 지정 지점에서 스스로 SIGKILL (정리 코드가 전혀 실행되지 않는 kill -9와 같다)
        print(f"FAILPOINT {where}", flush=True)
        os.kill(os.getpid(), signal.SIGKILL)

    def flush():
        roll = random.random() if rows else 1.0
        cut = {"both": failpoint_prob / 2, "before": failpoint_prob, "after": 0.0}[failpoint_where]
        if roll < cut:
            failpoint("before_insert")
        if rows:
            s = dict(settings)
            if dedup_token:
                s["insert_deduplication_token"] = ";".join(
                    f"{p}:{lo}-{hi}" for p, (lo, hi) in sorted(ranges.items()))
            t_batch = time.perf_counter()
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
            if prom:
                prom.update(st, len(rows), time.perf_counter() - t_batch)
            if cut <= roll < failpoint_prob:
                failpoint("after_insert_before_commit")
        dlq.flush()
        if prom and not rows:
            prom.update(st)  # DLQ로만 간 메시지도 반영
        if commit == "after" and ranges:
            c.commit(asynchronous=False)
        rows.clear()
        ranges.clear()

    last_flush = last_stat = time.time()
    sf = open(stats_path, "a") if stats_path else None
    while not stop:
        msg = c.poll(0.1)
        now = time.time()
        if msg is not None and not msg.error():
            clock["last_msg"] = now
            if clock["first_msg"] is None:
                clock["first_msg"] = now - t_start
                print(f"FIRST_MSG {clock['first_msg']:.2f}s", flush=True)
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
        if idle_exit and clock["assigned"] and now - clock["last_msg"] > idle_exit:
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
    ap.add_argument("--resume-from-table", action=argparse.BooleanOptionalAction, default=True)  # 실험 B 결과로 기본값
    ap.add_argument("--instance-id")
    ap.add_argument("--session-timeout-ms", type=int, default=45000)
    ap.add_argument("--failpoint-prob", type=float, default=0.0, help="실험용: 배치마다 이 확률로 강제 종료")
    ap.add_argument("--failpoint-where", choices=["both", "before", "after"], default="both")
    ap.add_argument("--metrics-port", type=int, default=0, help="Prometheus 지표 포트(0이면 끔)")
    ap.add_argument("--metrics-addr", default="127.0.0.1")
    a = ap.parse_args()
    print(json.dumps(run(a.table, a.group, a.batch_size, a.batch_ms, a.commit,
                         json.loads(a.settings), a.dedup_token, a.idle_exit, a.stats,
                         a.resume_from_table, a.instance_id, a.session_timeout_ms, a.failpoint_prob,
                         a.failpoint_where, a.metrics_port, a.metrics_addr)))

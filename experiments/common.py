"""실험 공용: 정답 계산, 프로세스 실행, 1초 간격 관측."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time

from confluent_kafka import Consumer, TopicPartition

from slp.infra import BOOTSTRAP, TOPIC, ch
from slp.validate import parse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable
ENV = {**os.environ, "PYTHONPATH": os.path.join(ROOT, "src")}


def truth(path: str, limit: int = 0) -> dict:
    valid, rejected, n = set(), 0, 0
    with open(path, "rb") as f:
        for line in f:
            row, err = parse(line.rstrip(b"\n").partition(b"\t")[2])
            if err:
                rejected += 1
            else:
                valid.add(str(row[0]))
            n += 1
            if limit and n >= limit:
                break
    return {"lines": n, "valid": len(valid), "rejected": rejected, "ids": valid}


def spawn(module: str, *args: str, log: str | None = None, env: dict | None = None) -> subprocess.Popen:
    out = open(log, "w") if log else subprocess.DEVNULL
    return subprocess.Popen([PY, "-m", module, *args], env={**ENV, **(env or {})}, cwd=ROOT, stdout=out,
                            stderr=subprocess.STDOUT)


def group_lag(group: str, partitions: int = 6) -> int:
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group, "enable.auto.commit": False})
    try:
        tps = [TopicPartition(TOPIC, p) for p in range(partitions)]
        committed = c.committed(tps, timeout=5)
        lag = 0
        for tp in committed:
            lo, hi = c.get_watermark_offsets(tp, timeout=5)
            lag += hi - (tp.offset if tp.offset >= 0 else lo)
        return lag
    finally:
        c.close()


class Sampler(threading.Thread):
    """1초마다 테이블 행 수, 활성 파트 수, 소비 그룹 lag를 기록한다."""

    def __init__(self, table: str, group: str | None):
        super().__init__(daemon=True)
        self.table, self.group, self.rows, self.stop_ev = table, group, [], threading.Event()
        self.t0 = time.time()

    def run(self):
        c = ch()
        while not self.stop_ev.is_set():
            r = c.query(f"""select (select count() from {self.table}),
                (select count() from system.parts where database='slp' and table='{self.table}' and active),
                (select count() from system.processes)""").result_rows[0]
            s = {"t": round(time.time() - self.t0, 1), "rows": r[0], "active_parts": r[1]}
            if self.group:
                try:
                    s["lag"] = group_lag(self.group)
                except Exception:  # noqa: BLE001
                    s["lag"] = None
            self.rows.append(s)
            self.stop_ev.wait(1)

    def stop(self):
        self.stop_ev.set()
        self.join()
        return self.rows


def latency(table: str) -> dict:
    """적재 지연 = inserted_at - Kafka 메시지 시각(ms)."""
    r = ch().query(f"""select quantiles(0.5, 0.95, 0.99)(dateDiff('millisecond', kafka_ts, inserted_at)),
                       max(dateDiff('millisecond', kafka_ts, inserted_at)) from {table}""").result_rows[0]
    return {"p50_ms": r[0][0], "p95_ms": r[0][1], "p99_ms": r[0][2], "max_ms": r[1]}


def save(name: str, obj, overwrite: bool = False) -> str:
    """결과 JSON을 저장한다. 같은 이름이 있으면 덮어쓰지 않고 시각을 붙인 새 파일에 쓴다.
    (다른 측정 환경에서 만든 원자료를 실수로 지우지 않기 위해. 덮어쓰려면 overwrite=True 또는 SLP_OVERWRITE=1)"""
    os.makedirs(os.path.join(ROOT, "docs", "results"), exist_ok=True)
    path = os.path.join(ROOT, "docs", "results", f"{name}.json")
    if os.path.exists(path) and not (overwrite or os.getenv("SLP_OVERWRITE") == "1"):
        path = path[:-5] + time.strftime(".conflict-%Y%m%dT%H%M%S.json")
        print(f"경고: {name}.json 이 이미 있어 {os.path.basename(path)} 에 저장", file=sys.stderr, flush=True)
    with open(path, "w") as f:
        json.dump(obj, f, ensure_ascii=False, indent=1, default=str)
    return path

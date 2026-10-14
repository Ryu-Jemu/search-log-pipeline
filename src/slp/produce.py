"""'키<TAB>JSON' 파일을 Kafka로 보낸다.

--rate N   : 초당 N건 (0이면 최대 속도)
--speed S  : 가상 시각 기준 S배속 재생. 지금까지 본 가장 늦은 event_time(워터마크)을 기준으로
             속도를 맞추므로, 늦게 도착하는 이벤트는 실제로도 늦게 도착한다(실험 C).
"""
import argparse
import json
import time
from datetime import datetime

from confluent_kafka import Producer

from slp.infra import BOOTSTRAP, TOPIC


def produce(path: str, rate: float = 0, limit: int = 0, speed: float = 0,
            sim0: str | None = None, start_at: float | None = None, until: str | None = None) -> dict:
    p = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True,
                  "linger.ms": 20, "compression.type": "lz4"})
    n, t0 = 0, time.time()
    if speed:
        s0 = datetime.fromisoformat(sim0).timestamp()
        end = datetime.fromisoformat(until).timestamp() if until else float("inf")
        wall0, wm = (start_at or t0), s0
        time.sleep(max(0.0, wall0 - time.time()))
    with open(path, "rb") as f:
        for line in f:
            key, _, value = line.rstrip(b"\n").partition(b"\t")
            if speed:
                try:
                    wm = max(wm, datetime.fromisoformat(json.loads(value)["event_time"]).timestamp())
                except (ValueError, KeyError, TypeError):
                    pass  # 형식 오류 이벤트는 현재 워터마크 시점에 보낸다
                if wm > end:
                    break
                ahead = wall0 + (wm - s0) / speed - time.time()
                if ahead > 0:
                    p.poll(0)
                    time.sleep(ahead)
            while True:
                try:
                    p.produce(TOPIC, value=value, key=key or None)
                    break
                except BufferError:  # 로컬 큐가 차면 비울 때까지 기다린다
                    p.poll(0.05)
            n += 1
            p.poll(0)
            if rate and n % 100 == 0:
                ahead = n / rate - (time.time() - t0)
                if ahead > 0:
                    time.sleep(ahead)
            if limit and n >= limit:
                break
    p.flush()
    dt = time.time() - t0
    return {"produced": n, "seconds": round(dt, 2), "per_s": round(n / dt)}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--rate", type=float, default=0)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--speed", type=float, default=0)
    ap.add_argument("--sim0", default="2026-01-05T00:00:00+00:00")
    ap.add_argument("--start-at", type=float)
    ap.add_argument("--until")
    a = ap.parse_args()
    print(json.dumps(produce(a.path, a.rate, a.limit, a.speed, a.sim0, a.start_at, a.until)))

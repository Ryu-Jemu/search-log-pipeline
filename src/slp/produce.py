"""'키<TAB>JSON' 파일을 Kafka로 보낸다. --rate 0 이면 최대 속도."""
import argparse
import json
import time

from confluent_kafka import Producer

from slp.infra import BOOTSTRAP, TOPIC


def produce(path: str, rate: float = 0, limit: int = 0) -> dict:
    p = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True,
                  "linger.ms": 20, "compression.type": "lz4"})
    n, t0 = 0, time.time()
    with open(path, "rb") as f:
        for line in f:
            key, _, value = line.rstrip(b"\n").partition(b"\t")
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
    a = ap.parse_args()
    print(json.dumps(produce(a.path, a.rate, a.limit)))

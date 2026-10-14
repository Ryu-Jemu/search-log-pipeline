"""로컬 Kafka·ClickHouse 접속과 토픽·테이블 초기화."""
from __future__ import annotations

import os
import time

import clickhouse_connect
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC, DLQ = "search-events", "search-events-dlq"

EVENTS_COLUMNS_DDL = """
    event_id UUID,
    event_type LowCardinality(String),
    event_time DateTime64(3, 'UTC'),
    user_id String,
    session_id String,
    qid UInt64,
    query String,
    query_norm String,
    doc_id Nullable(UInt32),
    kafka_ts DateTime64(3, 'UTC'),
    kafka_partition UInt16,
    kafka_offset UInt64,
    inserted_at DateTime64(3, 'UTC') DEFAULT now64(3)
"""


def ch():
    return clickhouse_connect.get_client(
        host=os.getenv("CH_HOST", "localhost"), port=8123, username="slp",
        password=os.getenv("CLICKHOUSE_PASSWORD", "slp_local_only"), database="slp")


def create_events_table(client, name: str, engine: str = "MergeTree", settings: str = "") -> None:
    client.command(f"DROP TABLE IF EXISTS {name} SYNC")
    client.command(f"""CREATE TABLE {name} ({EVENTS_COLUMNS_DDL})
        ENGINE = {engine}
        PARTITION BY toYYYYMMDD(event_time)
        ORDER BY (event_type, toStartOfHour(event_time), event_id)
        {settings}""")


def reset_topics(partitions: int = 6) -> None:
    """실험마다 토픽을 새로 만든다(오프셋·소비 그룹 상태를 남기지 않기 위해)."""
    a = AdminClient({"bootstrap.servers": BOOTSTRAP})
    existing = set(a.list_topics(timeout=10).topics)
    gone = [t for t in (TOPIC, DLQ) if t in existing]
    if gone:
        for f in a.delete_topics(gone).values():
            f.result()
        while set(gone) & set(a.list_topics(timeout=10).topics):
            time.sleep(0.5)
    for _ in range(20):  # 삭제 직후 재생성은 잠시 실패할 수 있다
        try:
            for f in a.create_topics([NewTopic(TOPIC, partitions, 1), NewTopic(DLQ, 1, 1)]).values():
                f.result()
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("토픽 생성 실패")

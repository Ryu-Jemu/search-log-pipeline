"""slp.metrics의 워터마크·버전·확정 후 늦은 도착 로직을 작은 고정 데이터로 검증한다.

ClickHouse가 떠 있어야 한다(docker compose up). 없으면 skip한다. 임시 DB slp_test_metrics를 쓰고 지운다.
파티션 3개(p0~p2)로 줄인 시나리오:
  1차 적재 → 잠정(v1) → 늦은 검색·다음 시간 클릭 적재 → 확정(v2) → 확정 뒤 늦은 클릭 적재 → 검사·재확정(v3)
"""
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from slp import metrics
from slp.infra import create_events_table
from slp.validate import COLUMNS

H = datetime(2026, 1, 5, tzinfo=timezone.utc)
DB = "slp_test_metrics"
P = 3  # 이 시험의 파티션 수


@pytest.fixture(scope="module")
def client():
    from slp.infra import ch
    try:
        admin = ch()
        admin.command("SELECT 1")
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"ClickHouse에 연결할 수 없다: {e}")
    admin.command(f"DROP DATABASE IF EXISTS {DB} SYNC")
    admin.command(f"CREATE DATABASE {DB}")
    c = ch(database=DB)
    create_events_table(c, "events")
    metrics.create_tables(c)
    yield c
    admin.command(f"DROP DATABASE IF EXISTS {DB} SYNC")


def ev(etype, minute, user, session, qid, part, off, doc=None):
    t = H + timedelta(minutes=minute)
    return [uuid.uuid4(), etype, t, user, session, qid, f"query {qid}", f"query {qid}", doc, t, part, off]


def put(c, rows):
    c.insert("events", rows, column_names=COLUMNS + ["kafka_ts", "kafka_partition", "kafka_offset"])
    time.sleep(0.01)  # 연속 INSERT의 inserted_at(ms)이 겹치지 않게


def test_watermark_versions_and_late_after_final(client):
    c = client
    put(c, [ev("search", 10, "u1", "s1", 1, 0, 0), ev("click", 11, "u1", "s1", 1, 0, 1, doc=5),
            ev("search", 20, "u2", "s2", 2, 1, 0), ev("search", 59.99, "u3", "s3", 3, 1, 1),
            ev("search", 30, "u1", "s4", 2, 2, 0)])
    assert not metrics.watermark_reached(c, H + timedelta(minutes=61), partitions=P)

    # p0은 다음 시간으로 넘어가고 p1에는 미래 시각 이상값이 들어와도, p2가 뒤처져 있으면 워터마크는 그대로다.
    put(c, [ev("search", 65, "u9", "s9", 9, 0, 2), ev("search", 60 * 24 * 27, "u8", "s8", 8, 1, 2)])
    assert not metrics.watermark_reached(c, H + timedelta(minutes=61), partitions=P)
    assert metrics.watermark(c, partitions=P) == H + timedelta(minutes=30)

    put(c, [ev("search", 62, "u7", "s7", 7, 2, 1)])
    assert metrics.watermark_reached(c, H + timedelta(minutes=61), partitions=P)
    assert not metrics.watermark_reached(c, H + timedelta(minutes=63), partitions=P)

    v1 = metrics.compute_hour(c, H, 1, "provisional", 0, partitions=P)
    assert (v1["searches"], v1["clicks"], v1["unique_users"], v1["zero_click_searches"]) == (4, 1, 3, 3)
    assert v1["offset_boundary"] == {0: 1, 1: 1, 2: 0}
    assert v1["watermark"] == H + timedelta(minutes=62)

    # 늦은 검색(00:40, p1 오프셋 3)과 s3·q3의 클릭(01:00:30, 다음 시간 이벤트)이 확정 전에 들어온다.
    put(c, [ev("search", 40, "u4", "s5", 1, 1, 3), ev("click", 60.5, "u3", "s3", 3, 1, 4, doc=3)])
    v2 = metrics.compute_hour(c, H, 2, "final", 1, partitions=P)
    assert (v2["searches"], v2["clicks"], v2["unique_users"], v2["zero_click_searches"]) == (5, 1, 4, 3)
    assert v2["offset_boundary"] == {0: 1, 1: 3, 2: 0}

    # 확정 뒤 그 시간의 클릭(00:45, p2 오프셋 2)이 들어온다 → 늦은 1건 감지, v3 재확정.
    put(c, [ev("click", 45, "u1", "s4", 2, 2, 2, doc=7)])
    r = metrics.late_after_final(c, H, partitions=P)
    assert (r["final_version"], r["late_events"], r["late_by_inserted_at"], r["refinalized_version"]) == (2, 1, 1, 3)
    v3 = r["refinalized"]
    assert (v3["searches"], v3["clicks"], v3["unique_users"], v3["zero_click_searches"]) == (5, 2, 4, 2)

    # 새로 들어온 것이 없으면 재확정하지 않는다(이번에는 v3 경계 기준).
    r2 = metrics.late_after_final(c, H, partitions=P)
    assert (r2["final_version"], r2["late_events"], r2["refinalized_version"]) == (3, 0, 0)

    latest = c.query("SELECT version, is_final, searches, clicks, ctr FROM hourly_metrics_latest").result_rows
    assert latest == [(3, 1, 5, 2, 0.4)]
    assert c.query("SELECT groupArray(version) FROM (SELECT version FROM hourly_metrics_versions ORDER BY version)"
                   ).result_rows[0][0] == [1, 2, 3]
    top = c.query("SELECT rank, query_norm, searches FROM hourly_top_queries FINAL ORDER BY rank").result_rows
    assert len(top) == metrics.TOP_N and top[:3] == [(1, "query 1", 2), (2, "query 2", 2), (3, "query 3", 1)]
    assert c.query("SELECT count() FROM hourly_late_checks").result_rows[0][0] == 2


def test_visible_condition_without_boundary():
    assert metrics._visible({}) == "0"

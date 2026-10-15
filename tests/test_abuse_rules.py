"""어뷰징 규칙·채점 시험. 채점 함수는 순수 파이썬, 규칙 SQL은 ClickHouse가 있을 때만(없으면 skip)."""
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from slp import abuse
from slp.infra import create_events_table
from slp.validate import COLUMNS

T0 = datetime(2026, 1, 5, tzinfo=timezone.utc)
DB = "slp_test_abuse"
LABELS = {"campaigns": [
    {"campaign_id": 1, "type": "query_flood", "user_ids": ["b1"], "query": "Flood Q",
     "start": "2026-01-05T10:00:00+00:00", "end": "2026-01-05T10:30:00+00:00"},
    {"campaign_id": 2, "type": "distributed_flood", "user_ids": ["d1", "d2", "d3"], "query": "dist q",
     "start": "2026-01-05T12:00:00+00:00", "end": "2026-01-05T12:40:00+00:00"}]}


def test_clopper_pearson_known_values():
    assert abuse.clopper_pearson(10, 10) == (0.6915, 1.0)
    assert abuse.clopper_pearson(0, 10) == (0.0, 0.3085)
    assert abuse.clopper_pearson(5, 10) == (0.1871, 0.8129)


def test_account_scores_counts_any_bot_as_true_positive():
    s = abuse.account_scores({"b1", "d1", "n1"}, LABELS, ("query_flood",))
    assert (s["flagged"], s["true_pos"], s["false_pos"]) == (3, 2, 1)
    assert s["recall_by_type"] == {"distributed_flood": {"hit": 1, "n": 3}, "query_flood": {"hit": 1, "n": 1}}
    assert s["target_recall"] == 1.0 and abs(s["target_f1"] - 0.8) < 1e-9


def test_campaign_scores_match_query_and_overlap():
    det = [("dist q", datetime(2026, 1, 5, 12, 30), set()),   # 구간 겹침 → 캠페인 2
           ("dist q", datetime(2026, 1, 5, 12, 40), set()),   # 끝 시각과 맞닿기만 함 → 오탐
           ("other", datetime(2026, 1, 5, 12, 0), set())]     # 다른 쿼리 → 오탐
    s = abuse.campaign_scores(det, LABELS)
    assert s["campaign_recall"] == {"hit": 1, "n": 1} and s["false_detections"] == 2


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
    yield c
    admin.command(f"DROP DATABASE IF EXISTS {DB} SYNC")


def ev(etype, t, user, q, doc=None):
    return [uuid.uuid4(), etype, t, user, f"s-{user}", abs(hash(q)) % 10**6, q, q, doc, t, 0, 0]


def test_rule_sql_on_fixed_data(client):
    c = client
    start = T0 + timedelta(hours=6)
    rows = []
    # 인기 쿼리: 시작 전 5시간 동안 10분마다 1건(이력 30건) → 평소값 30/144. 구간별 1건이라 최소 건수 조건보다 작다.
    rows += [ev("search", T0 + timedelta(hours=1, minutes=10 * i), f"n{i}", "popular q") for i in range(30)]
    # 시작 뒤 한 구간에 인기 쿼리 12건(12명), 희소 쿼리 12건(12명)
    rows += [ev("search", start + timedelta(minutes=1, seconds=i), f"p{i}", "popular q") for i in range(12)]
    rows += [ev("search", start + timedelta(minutes=2, seconds=i), f"d{i}", "dist q") for i in range(12)]
    # 봇: 5분 안에 같은 쿼리 25건, 같은 문서 클릭 6건
    rows += [ev("search", start + timedelta(minutes=20, seconds=10 * i), "bot", "flood q") for i in range(25)]
    rows += [ev("click", start + timedelta(minutes=30, seconds=10 * i), "bot", "flood q", doc=7) for i in range(6)]
    c.insert("events", rows, column_names=COLUMNS + ["kafka_ts", "kafka_partition", "kafka_offset"])
    end = start + timedelta(hours=1)

    r4 = {r[0]: r for r in abuse.r4_stats(c, "events", start, end, min_n=5, min_users=3, lam_min=0.1)}
    # 이력 구간(건수 1)이 최소 건수 조건에 걸려 빠지지 않고 평소값에 들어갔는지(필터가 윈도 함수 안으로 밀리지 않았는지)
    assert abs(r4["popular q"][4] - 30 / 144) < 1e-9
    assert r4["dist q"][4] == 0.1 and r4["dist q"][3] == 12
    assert abs(r4["dist q"][5] - (12 - 0.1) / 0.1 ** 0.5) < 1e-9

    r1 = abuse.r1_stats(c, "events", start, end, min_n=20)
    assert [(u, n) for u, _, n, _ in r1] == [("bot", 25)] and r1[0][3] == 1 / 25
    assert abuse.apply_r1(r1, 20, 0.5) == {"bot"} and abuse.apply_r1(r1, 30) == set()
    r3 = abuse.r3_stats(c, "events", start, end, min_c=2)
    assert [(u, m) for u, _, m in r3] == [("bot", 6)]
    r2 = abuse.r2_stats(c, "events", start, end, min_n=3)
    assert {u: (n, s) for u, _, n, s in r2} == {"bot": (25, 1.0)}

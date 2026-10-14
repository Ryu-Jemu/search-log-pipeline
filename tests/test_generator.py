import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from slp.generator import Config, Corpus, Generator


@pytest.fixture(scope="module")
def corpus(tmp_path_factory):
    # 쿼리 200개, 쿼리마다 문서 1~20개. 인기도 편차가 생기도록 문서 수를 다르게 둔다.
    rows = [{"qid": q, "query": f"query {q}", "n_docs": (q % 20) + 1,
             "docs": [q * 100 + d for d in range((q % 20) + 1)]} for q in range(200)]
    path = tmp_path_factory.mktemp("c") / "queries.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path)
    return Corpus.load(path)


def run(corpus, **kw):
    base = dict(sim_seconds=86400, sessions_per_day=3000, n_users=500, bad_fraction=0.0)
    g = Generator(corpus, Config(**{**base, **kw}))
    return g, list(g.events())


def test_same_seed_same_stream(corpus):
    assert run(corpus)[1] == run(corpus)[1]


def test_events_are_valid_and_unique(corpus):
    _, out = run(corpus)
    events = [json.loads(v) for _, v in out]
    assert len({e["event_id"] for e in events}) == len(events)
    docs_of = {q: set(range(q * 100, q * 100 + (q % 20) + 1)) for q in range(200)}
    for (key, _), e in zip(out, events):
        assert key == e["user_id"]
        if e["event_type"] == "click":
            assert e["doc_id"] in docs_of[e["qid"]]


def test_labels_match_emitted_bot_events(corpus):
    g, out = run(corpus)
    events = [json.loads(v) for _, v in out]
    assert {c["type"] for c in g.labels} == {"query_flood", "click_spam", "distributed_flood"}
    for c in g.labels:
        bot = set(c["user_ids"])
        mine = [e for e in events if e["user_id"] in bot]
        assert len(mine) == c["n_events"]
        assert {e["qid"] for e in mine} == {c["qid"]}


def test_diurnal_peak_exceeds_trough(corpus):
    _, out = run(corpus, query_floods=0, click_spams=0, distributed_floods=0)
    hours = [json.loads(v)["event_time"][11:13] for _, v in out]
    assert hours.count("16") > 2 * hours.count("04")


def test_bad_events_are_counted(corpus):
    g, out = run(corpus, bad_fraction=0.05)
    bad = 0
    for _, v in out:
        try:
            e = json.loads(v)
            bad += "user_id" not in e or e["event_time"] == "not-a-time"
        except json.JSONDecodeError:
            bad += 1
    assert bad == g.stats["bad"] > 0

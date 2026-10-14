import json

from slp.validate import normalize_query, parse

OK = {"event_id": "bcae8787-4197-27dd-e4e5-303a6532826c", "event_type": "search",
      "event_time": "2026-01-05T00:00:01.698+00:00", "user_id": "u0000033",
      "session_id": "s1", "qid": 1, "query": "  Wash  DOL\tlicense "}


def b(d):
    return json.dumps(d).encode()


def test_normalize():
    assert normalize_query("  Wash  DOL\tlicense ") == "wash dol license"


def test_valid_search_row():
    row, err = parse(b(OK))
    assert err is None and row[7] == "wash dol license" and row[8] is None


def test_rejections():
    cases = {
        b"{\"event_id\": \"x\"": "bad_json",
        b({**OK, "user_id": ""}): "bad_user_id",
        b({k: v for k, v in OK.items() if k != "user_id"}): "missing_user_id",
        b({**OK, "event_time": "not-a-time"}): "bad_value",
        b({**OK, "query": "   "}): "empty_query",
        b({**OK, "event_type": "click"}): "missing_doc_id",
        b({**OK, "event_type": "view"}): "bad_event_type",
    }
    for raw, reason in cases.items():
        assert parse(raw) == (None, reason), raw

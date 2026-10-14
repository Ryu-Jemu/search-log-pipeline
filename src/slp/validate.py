"""이벤트 검증·정규화. 수집기와 정답 계산(experiments)이 같은 함수를 쓴다."""
from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone

COLUMNS = ["event_id", "event_type", "event_time", "user_id", "session_id",
           "qid", "query", "query_norm", "doc_id"]
_WS = re.compile(r"\s+")


def normalize_query(q: str) -> str:
    return _WS.sub(" ", q).strip().lower()


def parse(value: bytes) -> tuple[list | None, str | None]:
    """(행, None) 또는 (None, 거부 사유)."""
    try:
        e = json.loads(value)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, "bad_json"
    if not isinstance(e, dict):
        return None, "bad_json"
    try:
        etype = e["event_type"]
        if etype not in ("search", "click"):
            return None, "bad_event_type"
        user = e["user_id"]
        if not isinstance(user, str) or not user:
            return None, "bad_user_id"
        t = datetime.fromisoformat(e["event_time"]).astimezone(timezone.utc)
        doc = e.get("doc_id")
        if etype == "click" and not isinstance(doc, int):
            return None, "missing_doc_id"
        q = e["query"]
        qn = normalize_query(q)
        if not qn:
            return None, "empty_query"
        return [uuid.UUID(e["event_id"]), etype, t, user, e["session_id"],
                int(e["qid"]), q, qn, doc], None
    except KeyError as k:
        return None, f"missing_{k.args[0]}"
    except (ValueError, TypeError):
        return None, "bad_value"

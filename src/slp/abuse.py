"""어뷰징 탐지 규칙(ClickHouse SQL, 시간 창 기반)과 채점 함수.

규칙마다 창 안의 통계를 SQL로 뽑고(후보), 임계값은 파이썬에서 적용한다. 같은 통계로 임계값 격자를 빠르게
훑을 수 있고, 운영(DAG)에서는 같은 함수에 고정 임계값을 넣어 쓴다.
  R1 burst   : 사용자 × 5분 창 검색 수(+ 선택: 서로 다른 쿼리 비율 상한)
  R2 repeat  : 사용자 × 1시간 창에서 가장 많이 검색한 쿼리의 비율(최소 검색 수 조건)
  R3 click   : 사용자 × 1시간 창에서 같은 문서 클릭 수 최댓값
  R4 spike   : 쿼리 × 10분 구간 검색 수·고유 사용자 수, 평소(직전 24시간, 해당 구간 제외) 대비 z
라벨(labels_*.json)은 채점에만 쓴다. 이벤트에는 라벨이 없다.
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta

from slp.validate import normalize_query


def _ts(t: datetime) -> str:
    return f"toDateTime('{t:%Y-%m-%d %H:%M:%S}', 'UTC')"


# ---------------------------------------------------------------- 규칙 통계(후보) SQL
def r1_stats(client, table: str, start: datetime, end: datetime, min_n: int, window_min: int = 5) -> list[tuple]:
    """(user_id, window, searches, distinct_ratio)"""
    return client.query(f"""
        SELECT user_id, toStartOfInterval(event_time, INTERVAL {window_min} MINUTE) w, count() n,
               uniqExact(query_norm) / count() AS distinct_ratio
        FROM {table} WHERE event_type = 'search' AND event_time >= {_ts(start)} AND event_time < {_ts(end)}
        GROUP BY user_id, w HAVING n >= {int(min_n)}""").result_rows


def r2_stats(client, table: str, start: datetime, end: datetime, min_n: int, window_min: int = 60) -> list[tuple]:
    """(user_id, window, searches, top_query_share)"""
    return client.query(f"""
        SELECT user_id, w, sum(c) n, max(c) / sum(c) AS top_share FROM (
            SELECT user_id, toStartOfInterval(event_time, INTERVAL {window_min} MINUTE) w, query_norm, count() c
            FROM {table} WHERE event_type = 'search' AND event_time >= {_ts(start)} AND event_time < {_ts(end)}
            GROUP BY user_id, w, query_norm)
        GROUP BY user_id, w HAVING n >= {int(min_n)}""").result_rows


def r3_stats(client, table: str, start: datetime, end: datetime, min_c: int, window_min: int = 60) -> list[tuple]:
    """(user_id, window, max_same_doc_clicks)"""
    return client.query(f"""
        SELECT user_id, w, max(c) m FROM (
            SELECT user_id, toStartOfInterval(event_time, INTERVAL {window_min} MINUTE) w, doc_id, count() c
            FROM {table} WHERE event_type = 'click' AND event_time >= {_ts(start)} AND event_time < {_ts(end)}
            GROUP BY user_id, w, doc_id)
        GROUP BY user_id, w HAVING m >= {int(min_c)}""").result_rows


def r4_stats(client, table: str, start: datetime, end: datetime, min_n: int, min_users: int,
             lam_min: float = 0.1, bucket_min: int = 10, history_h: int = 24) -> list[tuple]:
    """(query_norm, bucket, searches, users, baseline_lambda, z, user_ids)

    baseline = 직전 history_h시간(현재 구간 제외) 같은 쿼리 검색 수 / 구간 수, 하한 lam_min(포아송 근사 z).
    표에 이력이 없는 앞부분은 하한이 쓰인다(문서에 한계로 기록)."""
    per_day = history_h * 60 // bucket_min
    sec = bucket_min * 60
    return client.query(f"""
        SELECT query_norm, b, n, u, lam, (n - lam) / sqrt(lam) AS z, users FROM (
            SELECT query_norm, b, n, u, users,
                   greatest(sum(n) OVER (PARTITION BY query_norm ORDER BY toUInt32(b)
                                         RANGE BETWEEN {history_h * 3600} PRECEDING AND {sec} PRECEDING)
                            / {per_day}, {float(lam_min)}) AS lam
            FROM (
                SELECT query_norm, toStartOfInterval(event_time, INTERVAL {bucket_min} MINUTE) b, count() n,
                       uniqExact(user_id) u, groupUniqArray(user_id) users
                FROM {table}
                WHERE event_type = 'search' AND event_time >= {_ts(start - timedelta(hours=history_h))}
                  AND event_time < {_ts(end)}
                GROUP BY query_norm, b))
        WHERE b >= {_ts(start)} AND n >= {int(min_n)} AND u >= {int(min_users)}""").result_rows


# ---------------------------------------------------------------- 임계값 적용
def apply_r1(rows, t: int, max_distinct: float = 1.0) -> set[str]:
    return {r[0] for r in rows if r[2] >= t and r[3] <= max_distinct}


def apply_r2(rows, min_n: int, share: float) -> set[str]:
    return {r[0] for r in rows if r[2] >= min_n and r[3] >= share}


def apply_r3(rows, c: int) -> set[str]:
    return {r[0] for r in rows if r[2] >= c}


def apply_r4(rows, n: int, users: int, z: float) -> list[tuple]:
    """탐지된 (query_norm, bucket, user_ids) 목록"""
    return [(r[0], r[1], set(r[6])) for r in rows if r[2] >= n and r[3] >= users and r[5] >= z]


# ---------------------------------------------------------------- 채점
def bots_by_type(labels: dict) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for c in labels["campaigns"]:
        out.setdefault(c["type"], set()).update(c["user_ids"])
    return out


def account_scores(flagged: set[str], labels: dict, target_types: tuple[str, ...] | None = None) -> dict:
    """계정 단위 채점. 정밀도는 모든 봇을 정답으로 본다(다른 유형 봇을 잡아도 오탐이 아니다).
    재현율은 유형별, 그리고 target_types 합계."""
    by = bots_by_type(labels)
    bots = set().union(*by.values())
    tp = flagged & bots
    out = {"flagged": len(flagged), "true_pos": len(tp), "false_pos": len(flagged - bots),
           "precision": len(tp) / len(flagged) if flagged else None,
           "recall_by_type": {t: {"hit": len(flagged & u), "n": len(u)} for t, u in sorted(by.items())}}
    if target_types:
        tgt = set().union(*(by.get(t, set()) for t in target_types))
        r = len(flagged & tgt) / len(tgt) if tgt else None
        p = out["precision"]
        out["target_types"] = list(target_types)
        out["target_recall"] = r
        out["target_f1"] = (2 * p * r / (p + r)) if p and r else 0.0
    return out


def campaign_scores(detections: list[tuple], labels: dict, kinds: tuple[str, ...] = ("distributed_flood",),
                    bucket_min: int = 10) -> dict:
    """R4 캠페인 단위 채점: 쿼리(정규화)가 같고 구간이 캠페인 [start, end]와 겹치면 그 캠페인을 잡은 것.
    탐지 정밀도 = 어떤 캠페인(유형 무관)과도 겹치지 않는 탐지 구간을 오탐으로 본 비율."""
    camps = [(normalize_query(c["query"]), datetime.fromisoformat(c["start"]), datetime.fromisoformat(c["end"]),
              c["type"], c["campaign_id"]) for c in labels["campaigns"]]

    def hit(q, b, c):
        b = b if b.tzinfo else b.replace(tzinfo=c[1].tzinfo)
        return q == c[0] and b < c[2] and b + timedelta(minutes=bucket_min) > c[1]

    caught = {c[4] for q, b, _ in detections for c in camps if hit(q, b, c)}
    target = [c for c in camps if c[3] in kinds]
    false_det = [(q, str(b)) for q, b, _ in detections if not any(hit(q, b, c) for c in camps)]
    return {"detections": len(detections), "false_detections": len(false_det), "false_examples": false_det[:10],
            "campaign_recall": {"hit": sum(c[4] in caught for c in target), "n": len(target)},
            "detection_precision": (1 - len(false_det) / len(detections)) if detections else None}


def clopper_pearson(k: int, n: int, alpha: float = 0.05) -> tuple[float, float]:
    """이항 비율 k/n의 정확한(Clopper-Pearson) 신뢰구간. scipy 없이 이분 탐색."""
    if n == 0:
        return (0.0, 1.0)

    def cdf(x, p):  # P(X <= x)
        return sum(math.comb(n, i) * p ** i * (1 - p) ** (n - i) for i in range(0, x + 1))

    def solve(f, lo=0.0, hi=1.0):
        for _ in range(100):
            mid = (lo + hi) / 2
            if f(mid):
                hi = mid
            else:
                lo = mid
        return (lo + hi) / 2

    lower = 0.0 if k == 0 else solve(lambda p: 1 - cdf(k - 1, p) >= alpha / 2)
    upper = 1.0 if k == n else solve(lambda p: cdf(k, p) <= alpha / 2)
    return (round(lower, 4), round(upper, 4))


"""03 문서의 진단 수치(오탐 계정 특성, 놓친 계정·캠페인의 원인, R4의 z 동작)를 원자료로 남긴다.

채점이 끝난 뒤의 사후 진단이다. 임계값은 바꾸지 않는다(abuse.CHOSEN = day1 결정값).
보류 seed의 라벨을 읽지만 진단 설명에만 쓴다.
"""
import json
import os
from datetime import datetime, timedelta, timezone

from common import ROOT, save

from slp import abuse
from slp.infra import ch
from slp.validate import normalize_query

SIM0 = datetime(2026, 1, 5, tzinfo=timezone.utc)
START, END = SIM0, SIM0 + timedelta(hours=25)
HOLDOUT = [43, 44, 45, 46, 47]


def labels(seed: int) -> dict:
    name = "labels_day1.json" if seed == 42 else f"labels_s{seed}.json"
    return json.load(open(os.path.join(ROOT, "data", name)))


def top_users(c) -> list[dict]:
    rows = c.query("""SELECT user_id, countIf(event_type = 'search') s,
                             uniqExactIf(query_norm, event_type = 'search') dq, uniqExact(query_norm) dq_all
                      FROM events_s42 WHERE user_id IN ('u0000000', 'u0000001', 'u0000002')
                      GROUP BY user_id ORDER BY user_id""").result_rows
    r1 = abuse.r1_stats(c, "events_s42", START, END, 1)
    return [{"user_id": u, "searches": s, "distinct_queries_in_searches": dq, "distinct_queries_all_events": da,
             "max_searches_5min": max(r[2] for r in r1 if r[0] == u)} for u, s, dq, da in rows]


def bots_5min(c) -> dict:
    by = abuse.bots_by_type(labels(42))
    r1 = abuse.r1_stats(c, "events_s42", START, END, 1)
    out = {t: sorted(max(r[2] for r in r1 if r[0] == u) for u in by[t]) for t in ("query_flood", "click_spam")}
    bots = by["query_flood"] | by["click_spam"]
    w = [r[3] for r in r1 if r[0] in bots and r[2] >= 70]
    out["windows_ge_70"] = {"n": len(w), "distinct_ratio_min": min(w), "distinct_ratio_max": max(w)}
    return out


def r2_missed(c) -> dict:
    hist: dict = {}
    for s in HOLDOUT:
        st = abuse.r2_stats(c, f"events_s{s}", START, END, 1)
        flagged = abuse.apply_r2(st, **abuse.CHOSEN["R2"])
        for camp in labels(s)["campaigns"]:
            if camp["type"] != "distributed_flood":
                continue
            for u in camp["user_ids"]:
                if u in flagged:
                    continue
                mine = [r[2] for r in st if r[0] == u]
                key = f"total={sum(mine)},max_per_1h_window={max(mine)}"
                hist[key] = hist.get(key, 0) + 1
    return hist


def campaigns(c) -> list[dict]:
    out = []
    for s in [42] + HOLDOUT:
        st = abuse.r4_stats(c, f"events_s{s}", START, END, 1, 1, abuse.LAM_MIN)
        caught = abuse.apply_r4([r for r in st if r[2] >= abuse.CHOSEN["R4"]["n"] and r[3] >= abuse.CHOSEN["R4"]["users"]],
                                **abuse.CHOSEN["R4"])
        for camp in labels(s)["campaigns"]:
            if camp["type"] != "distributed_flood":
                continue
            q = normalize_query(camp["query"])
            st_c, en_c = datetime.fromisoformat(camp["start"]), datetime.fromisoformat(camp["end"])
            buckets = [r for r in st if r[0] == q]
            out.append({"seed": s, "campaign_id": camp["campaign_id"], "accounts": len(camp["user_ids"]),
                        "events": camp["n_events"], "minutes": round((en_c - st_c).total_seconds() / 60),
                        "max_searches_per_10min": max(r[2] for r in buckets),
                        "max_users_per_10min": max(r[3] for r in buckets),
                        "buckets": [{"bucket": str(r[1])[11:16], "n": r[2], "users": r[3], "lambda": round(r[4], 3),
                                     "z": round(r[5], 1)} for r in sorted(buckets, key=lambda x: x[1]) if r[2] >= 5],
                        "caught_by_chosen_R4": any(d[0] == q for d in caught)})
    return out


def r4_day1_candidates(c) -> dict:
    lab = labels(42)
    camp_q = {normalize_query(x["query"]) for x in lab["campaigns"]}
    rows = abuse.r4_stats(c, "events_s42", START, END, 5, 3, abuse.LAM_MIN)
    return {"n": len(rows), "in_campaign_queries": sum(r[0] in camp_q for r in rows),
            "lambda_above_floor": sum(r[4] > abuse.LAM_MIN + 1e-12 for r in rows),
            "lambda_range": [round(min(r[4] for r in rows), 3), round(max(r[4] for r in rows), 3)],
            "z_min": round(min(r[5] for r in rows), 1),
            "note": "R4 후보 조건(n≥5, 고유 사용자≥3)을 넘은 day1 구간 전체"}


def query_frequency(c) -> dict:
    n, le1 = c.query("""SELECT count(), countIf(c <= 1) FROM (
        SELECT query_norm, count() c FROM events_s42 WHERE event_type = 'search' GROUP BY query_norm)""").result_rows[0]
    return {"distinct_searched_queries": n, "searched_at_most_once": le1, "share": round(le1 / n, 4)}


if __name__ == "__main__":
    c = ch()
    res = {"note": "채점 뒤 사후 진단(임계값 변경 없음). abuse.CHOSEN = " + json.dumps(abuse.CHOSEN),
           "day1_top_normal_users": top_users(c), "day1_bot_5min": bots_5min(c),
           "holdout_r2_missed_distributed_accounts": r2_missed(c), "distributed_campaigns": campaigns(c),
           "day1_r4_candidates": r4_day1_candidates(c), "day1_query_frequency": query_frequency(c)}
    print(json.dumps({k: v for k, v in res.items() if k != "distributed_campaigns"}, ensure_ascii=False, indent=1))
    for x in res["distributed_campaigns"]:
        print(x["seed"], x["campaign_id"], x["accounts"], x["events"], x["minutes"], x["max_searches_per_10min"],
              x["max_users_per_10min"], x["caught_by_chosen_R4"], [(b["bucket"], b["n"], b["lambda"], b["z"]) for b in x["buckets"]])
    print(save("abuse_diagnostics_mac_m4", res))

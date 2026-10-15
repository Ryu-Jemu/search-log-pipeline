"""어뷰징 탐지 규칙 R1~R4의 임계값 결정(day1)과 보류 데이터 채점.

데이터 누수 방지를 위해 세 단계를 따로 실행한다.
  load     : seed별 하루치를 만들고(seed 42는 기존 day1.tsv·labels_day1.json) Kafka → 수집기로 events_s{seed}에 적재
  tune     : events_s42(day1)와 labels_day1.json만 읽어 임계값 격자를 훑고 고른다 → abuse_thresholds_day1_*.json
  evaluate : 고른 임계값을 고정한 채 보류 seed 표에 적용해 채점한다 → abuse_holdout_*.json
가상 날짜가 seed마다 같으므로(2026-01-05) 표를 seed별로 나눠 섞이지 않게 한다.
봇 계정 번호는 정상 사용자 범위(u0000000~u0099999) 뒤부터 매겨진다. 규칙은 user_id 값을 쓰지 않는다.
"""
import argparse
import itertools
import json
import os
import subprocess
import time
from datetime import datetime, timedelta, timezone

from common import ENV, PY, ROOT, save, spawn, truth

from slp import abuse
from slp.infra import ch, create_events_table, reset_topics

SIM0 = datetime(2026, 1, 5, tzinfo=timezone.utc)
START, END = SIM0, SIM0 + timedelta(hours=25)  # 다음 날로 넘어간 꼬리 이벤트까지
GRID = {
    "R1": {"t": [20, 30, 40, 50, 60, 70, 80, 90, 100, 120], "max_distinct": [1.0, 0.5, 0.2]},
    "R2": {"min_n": [3, 4, 5, 6, 8, 10, 15, 20, 30], "share": [0.5, 0.7, 0.9, 1.0]},
    "R3": {"c": [2, 3, 4, 5, 10, 20, 50]},
    "R4": {"n": [5, 10, 15, 20, 30, 50], "users": [3, 5, 10, 15, 20, 30], "z": [0, 3, 10, 30]},
}
TARGET = {"R1": ("query_flood", "click_spam"), "R2": ("query_flood", "click_spam", "distributed_flood"),
          "R3": ("click_spam",), "R4": ("distributed_flood",)}
LAM_MIN = 0.1


def paths(seed: int) -> tuple[str, str]:
    if seed == 42:
        return os.path.join(ROOT, "data", "day1.tsv"), os.path.join(ROOT, "data", "labels_day1.json")
    return os.path.join(ROOT, "data", f"s{seed}.tsv"), os.path.join(ROOT, "data", f"labels_s{seed}.json")


def load(seeds: list[int]) -> dict:
    out = {}
    c = ch()
    for s in seeds:
        data, labels = paths(s)
        if not os.path.exists(data):
            r = subprocess.run([PY, "-m", "slp.generator", "--hours", "24", "--seed", str(s), "--labels", labels],
                               env=ENV, cwd=ROOT, stdout=open(data, "wb"), stderr=subprocess.PIPE, check=True)
            print(s, r.stderr.decode().strip(), flush=True)
        table = f"events_s{s}"
        reset_topics()
        create_events_table(c, table)
        spawn("slp.produce", data, log=os.path.join(ROOT, "data", f"abuse_load_{s}_p.log")).wait()
        spawn("slp.consume", "--table", table, "--group", f"abuse-{s}-{int(time.time())}", "--instance-id",
              f"ingest-abuse-{s}", "--idle-exit", "5", log=os.path.join(ROOT, "data", f"abuse_load_{s}_c.log")).wait()
        tr = truth(data)
        rows, uniq = c.query(f"SELECT count(), uniqExact(event_id) FROM {table}").result_rows[0]
        out[s] = {"table": table, "truth_valid": tr["valid"], "rows": rows, "unique": uniq,
                  "campaigns": len(json.load(open(labels))["campaigns"])}
        print(s, out[s], flush=True)
    return out


def stats(c, table: str) -> dict:
    return {"R1": abuse.r1_stats(c, table, START, END, min(GRID["R1"]["t"])),
            "R2": abuse.r2_stats(c, table, START, END, min(GRID["R2"]["min_n"])),
            "R3": abuse.r3_stats(c, table, START, END, min(GRID["R3"]["c"])),
            "R4": abuse.r4_stats(c, table, START, END, min(GRID["R4"]["n"]), min(GRID["R4"]["users"]), LAM_MIN)}


def score(rule: str, st: dict, p: dict, labels: dict) -> dict:
    if rule == "R1":
        return abuse.account_scores(abuse.apply_r1(st["R1"], p["t"], p["max_distinct"]), labels, TARGET["R1"])
    if rule == "R2":
        return abuse.account_scores(abuse.apply_r2(st["R2"], p["min_n"], p["share"]), labels, TARGET["R2"])
    if rule == "R3":
        return abuse.account_scores(abuse.apply_r3(st["R3"], p["c"]), labels, TARGET["R3"])
    det = abuse.apply_r4(st["R4"], p["n"], p["users"], p["z"])
    out = abuse.campaign_scores(det, labels)
    accts = set().union(*(u for _, _, u in det)) if det else set()
    out["accounts"] = abuse.account_scores(accts, labels, TARGET["R4"])
    return out


def key(rule: str, s: dict, p: dict) -> tuple:
    """격자 선택 기준(day1에서만 사용). 같으면 더 보수적인(큰) 임계값."""
    if rule == "R4":
        r = s["campaign_recall"]["hit"] / s["campaign_recall"]["n"]
        prec = s["detection_precision"] or 0.0
        f1 = 2 * prec * r / (prec + r) if prec + r else 0.0
        return (f1, prec, p["n"], p["users"], p["z"])
    conservative = {"R1": (p.get("t"), -p.get("max_distinct", 0)), "R2": (p.get("min_n"), p.get("share")),
                    "R3": (p.get("c"),)}[rule]  # R1의 max_distinct는 작을수록 엄격하다
    return (s["target_f1"], s["precision"] or 0.0, *conservative)


def tune() -> dict:
    c = ch()
    labels = json.load(open(paths(42)[1]))
    st = stats(c, "events_s42")
    chosen, grids = {}, {}
    for rule, g in GRID.items():
        rows = []
        for combo in itertools.product(*g.values()):
            p = dict(zip(g.keys(), combo))
            rows.append({"params": p, "score": score(rule, st, p, labels)})
        best = max(rows, key=lambda r: key(rule, r["score"], r["params"]))
        chosen[rule] = best["params"]
        grids[rule] = rows
        print(rule, best["params"], json.dumps(best["score"], ensure_ascii=False)[:300], flush=True)
    return {"tables_read": ["events_s42"], "labels_read": [os.path.basename(paths(42)[1])], "window": [str(START),
            str(END)], "lam_min": LAM_MIN, "target_types": TARGET, "selection": (
                "R1~R3: 목표 유형 재현율과 정밀도(모든 봇을 정답으로 봄)의 F1 최대, 같으면 정밀도·큰 임계값. "
                "R4: 캠페인 재현율과 탐지 구간 정밀도의 F1 최대, 같으면 정밀도·큰 임계값"),
            "chosen": chosen, "grid": grids}


def fp_samples(c, table: str, users: list[str]) -> list[dict]:
    if not users:
        return []
    ids = ",".join(f"'{u}'" for u in users)
    rows = c.query(f"""SELECT user_id, countIf(event_type = 'search'), countIf(event_type = 'click'),
                              uniqExact(query_norm), uniqExact(session_id) FROM {table}
                       WHERE user_id IN ({ids}) GROUP BY user_id ORDER BY user_id""").result_rows
    return [{"user_id": u, "activity_rank": int(u[1:]) + 1, "searches": s, "clicks": k, "distinct_queries": q,
             "sessions": ss} for u, s, k, q, ss in rows]


def evaluate(seeds: list[int], thresholds_path: str) -> dict:
    th = json.load(open(thresholds_path))
    if th["tables_read"] != ["events_s42"]:  # assert가 아니라 예외: python -O에서도 검사한다
        raise SystemExit("임계값은 day1(events_s42)에서만 정해야 한다")
    chosen = th["chosen"]
    c = ch()
    per_seed, pooled = {}, {}
    sweeps = {"R1": "t", "R2": "min_n", "R3": "c", "R4": "n"}
    for s in seeds:
        if s == 42:
            raise SystemExit("보류 데이터에 day1(seed 42)을 넣지 않는다")
        table = f"events_s{s}"
        labels = json.load(open(paths(s)[1]))
        st = stats(c, table)
        res = {r: score(r, st, chosen[r], labels) for r in GRID}
        bots = set().union(*abuse.bots_by_type(labels).values())
        union = (abuse.apply_r1(st["R1"], **chosen["R1"]) | abuse.apply_r2(st["R2"], **chosen["R2"])
                 | abuse.apply_r3(st["R3"], **chosen["R3"]))
        det4 = abuse.apply_r4(st["R4"], **chosen["R4"])
        union4 = union | (set().union(*(u for _, _, u in det4)) if det4 else set())
        res["R1|R2|R3"] = abuse.account_scores(union, labels)
        res["R1|R2|R3|R4accounts"] = abuse.account_scores(union4, labels)
        fps = {}
        for r, f in (("R1", abuse.apply_r1(st["R1"], **chosen["R1"])), ("R2", abuse.apply_r2(st["R2"], **chosen["R2"])),
                     ("R3", abuse.apply_r3(st["R3"], **chosen["R3"]))):
            fps[r] = fp_samples(c, table, sorted(f - bots)[:5])
        res["false_positive_samples"] = fps
        # 표시용 곡선(주 임계값만 바꾸고 나머지는 고정). 임계값 선택에는 쓰지 않는다.
        res["curves"] = {r: [{"value": v, "score": score(r, st, {**chosen[r], sweeps[r]: v}, labels)}
                             for v in GRID[r][sweeps[r]]] for r in GRID}
        per_seed[s] = res
        print(s, {r: (res[r].get("precision"), res[r].get("recall_by_type")) for r in ("R1", "R2", "R3")},
              res["R4"]["campaign_recall"], flush=True)
    # 합산: seed마다 계정 번호가 겹치므로 집합이 아니라 건수를 더한다.
    for r in ("R1", "R2", "R3", "R1|R2|R3", "R1|R2|R3|R4accounts"):
        tp = sum(per_seed[s][r]["true_pos"] for s in seeds)
        fl = sum(per_seed[s][r]["flagged"] for s in seeds)
        rec = {}
        for t in ("query_flood", "click_spam", "distributed_flood"):
            hit = sum(per_seed[s][r]["recall_by_type"].get(t, {}).get("hit", 0) for s in seeds)
            n = sum(per_seed[s][r]["recall_by_type"].get(t, {}).get("n", 0) for s in seeds)
            rec[t] = {"hit": hit, "n": n, "recall": round(hit / n, 4) if n else None,
                      "ci95": abuse.clopper_pearson(hit, n)}
        pooled[r] = {"flagged": fl, "true_pos": tp, "precision": round(tp / fl, 4) if fl else None,
                     "precision_ci95": abuse.clopper_pearson(tp, fl), "recall_by_type": rec}
    hit = sum(per_seed[s]["R4"]["campaign_recall"]["hit"] for s in seeds)
    n = sum(per_seed[s]["R4"]["campaign_recall"]["n"] for s in seeds)
    det = sum(per_seed[s]["R4"]["detections"] for s in seeds)
    fd = sum(per_seed[s]["R4"]["false_detections"] for s in seeds)
    pooled["R4"] = {"campaign_recall": {"hit": hit, "n": n, "ci95": abuse.clopper_pearson(hit, n)},
                    "detections": det, "false_detections": fd,
                    "detection_precision_ci95": abuse.clopper_pearson(det - fd, det)}
    return {"thresholds_file": os.path.basename(thresholds_path), "chosen": chosen, "seeds": seeds,
            "pooled": pooled, "per_seed": per_seed}


def hourly_equivalence(table: str = "events_s42") -> dict:
    """운영용 시간 단위 탐지(abuse.detect_hour)를 25시간 돌린 합이 하루 단위 결과와 같은지 확인한다(채점 아님)."""
    th = json.load(open(os.path.join(ROOT, "docs", "results", "abuse_thresholds_day1_mac_m4.json")))
    assert abuse.CHOSEN == th["chosen"], "abuse.CHOSEN이 day1 결정값과 다르다"
    c = ch()
    st = stats(c, table)
    day = {"R1": abuse.apply_r1(st["R1"], **abuse.CHOSEN["R1"]), "R2": abuse.apply_r2(st["R2"], **abuse.CHOSEN["R2"]),
           "R3": abuse.apply_r3(st["R3"], **abuse.CHOSEN["R3"]),
           "R4": {(q, str(b)) for q, b, _ in abuse.apply_r4(st["R4"], **abuse.CHOSEN["R4"])}}
    hourly = {"R1": set(), "R2": set(), "R3": set(), "R4": set()}
    for h in range(25):
        for r in abuse.detect_hour(c, table, START + timedelta(hours=h)):
            hourly[r["rule"]].add((r["entity"], str(r["window"])) if r["rule"] == "R4" else r["entity"])
    return {"table": table, "hours": 25, "equal": {k: day[k] == hourly[k] for k in day},
            "counts": {k: {"day": len(day[k]), "hourly": len(hourly[k])} for k in day}}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("step", choices=["load", "tune", "evaluate", "hourly"])
    ap.add_argument("--seeds", type=int, nargs="*", default=[42, 43, 44, 45, 46, 47])
    ap.add_argument("--tag", default="mac_m4")
    a = ap.parse_args()
    if a.step == "load":
        print(save(f"abuse_load_{a.tag}", load(a.seeds)))
    elif a.step == "hourly":
        r = hourly_equivalence()
        print(r)
        print(save(f"abuse_hourly_equivalence_{a.tag}", r))
    elif a.step == "tune":
        print(save(f"abuse_thresholds_day1_{a.tag}", tune()))
    else:
        hold = [s for s in a.seeds if s != 42]
        th = os.path.join(ROOT, "docs", "results", f"abuse_thresholds_day1_{a.tag}.json")
        print(save(f"abuse_holdout_{a.tag}", evaluate(hold, th)))

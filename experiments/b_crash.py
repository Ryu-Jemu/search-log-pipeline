"""실험 B: 수집기를 무작위 시점에 kill -9 했을 때의 유실·중복과 멱등 적재 비교.

day1 전체(유효 709,950건)를 Kafka에 먼저 쌓고, 수집기를 띄웠다가 1.5~5초 뒤 SIGKILL하기를
KILLS번 반복한 뒤 마지막으로 끝까지 소비시킨다. 결과를 재생기 정답 event_id 집합과 대조한다.
"""
import argparse
import os
import random
import statistics
import time

from common import ROOT, save, spawn, truth

from confluent_kafka import Consumer, TopicPartition
from slp.infra import BOOTSTRAP, DLQ, ch, create_events_table, reset_topics

VARIANTS = {
    # 이름: (테이블 엔진, 테이블 설정, 수집기 인자)
    "b0_autocommit": ("MergeTree", "", ["--commit", "auto"]),
    "b1_commit_after_insert": ("MergeTree", "", ["--commit", "after"]),
    "b2_replacing_mergetree": ("ReplacingMergeTree(inserted_at)", "", ["--commit", "after"]),
    "b3a_dedup_token_default": ("MergeTree", "", ["--commit", "after", "--dedup-token"]),
    "b3b_dedup_token_window": ("MergeTree", "SETTINGS non_replicated_deduplication_window = 1000",
                               ["--commit", "after", "--dedup-token"]),
    "b4_resume_from_table": ("MergeTree", "", ["--commit", "after", "--resume-from-table"]),
    # 배치 창이 자동 커밋 주기(1초)보다 길면 유실 창이 넓어지는지 확인
    "b0b_autocommit_batch_2s": ("MergeTree", "", ["--commit", "auto", "--batch-ms", "2000"]),
    # 유실 경로만 겨냥: 적재 직전에만 죽인다
    "b0c_autocommit_before_insert_200ms": ("MergeTree", "", ["--commit", "auto", "--failpoint-where", "before"]),
    "b0d_autocommit_before_insert_2s": ("MergeTree", "", ["--commit", "auto", "--batch-ms", "2000",
                                                          "--failpoint-where", "before"]),
    "b1b_commit_after_before_insert_2s": ("MergeTree", "", ["--commit", "after", "--batch-ms", "2000",
                                                            "--failpoint-where", "before"]),
}


def dlq_count() -> int:
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "dlq-count"})
    lo, hi = c.get_watermark_offsets(TopicPartition(DLQ, 0), timeout=5)
    c.close()
    return hi - lo


def timed(c, sql, n=5):
    ts = []
    for _ in range(n):
        t = time.perf_counter()
        c.query(sql)
        ts.append((time.perf_counter() - t) * 1000)
    return round(statistics.median(ts), 1)


def run(name, engine, tbl_settings, args, data, tr, kills, seed, mode, fp_prob):
    rnd = random.Random(seed)
    c = ch()
    reset_topics()
    create_events_table(c, "events", engine, tbl_settings)
    spawn("slp.produce", data).wait()
    group = f"b-{name}-{int(time.time())}"
    log = os.path.join(ROOT, "data", f"b_{name}.log")
    killed = []
    for i in range(kills):
        # 정적 멤버십: 재시작 즉시 같은 파티션을 받게 해 kill이 처리 구간에 떨어지도록 한다(b_restart.py 참고)
        extra = ["--failpoint-prob", str(fp_prob)] if mode == "failpoint" else []
        lg = f"{log}.{i}"
        p = spawn("slp.consume", "--table", "events", "--group", group, "--idle-exit", "5",
                  "--instance-id", "ingest-0", *args, *extra, log=lg)
        if mode == "failpoint":
            p.wait()  # 수집기가 장애 지점에서 스스로 죽거나 끝까지 소비한다
            fp = [l.split()[1] for l in open(lg) if l.startswith("FAILPOINT")]
            if not fp:
                break
            killed.append(fp[0])
            continue
        wait = rnd.uniform(1.5, 5.0)
        try:
            p.wait(wait)
            break  # 끝까지 소비해 스스로 종료
        except Exception:  # noqa: BLE001 - TimeoutExpired
            p.kill()
            p.wait()
            killed.append(round(wait, 2))
    t0 = time.time()
    spawn("slp.consume", "--table", "events", "--group", group, "--idle-exit", "5",
          "--instance-id", "ingest-0", *args, log=log).wait()
    final_s = time.time() - t0
    got = {str(r[0]) for r in c.query("select distinct event_id from events").result_rows}
    rows, uniq = c.query("select count(), uniqExact(event_id) from events").result_rows[0]
    res = {
        "variant": name, "engine": engine, "table_settings": tbl_settings, "args": args,
        "mode": mode, "kills": len(killed), "kill_points": killed, "final_run_s": round(final_s, 1),
        "truth_valid": tr["valid"], "rows": rows, "unique": uniq,
        "lost": len(tr["ids"] - got), "unknown_ids": len(got - tr["ids"]),
        "duplicates": rows - uniq, "dlq_messages": dlq_count(), "truth_rejected": tr["rejected"],
    }
    if res["duplicates"]:
        # 중복 행이 어떤 INSERT(같은 inserted_at)에서 어떤 오프셋 구간으로 들어갔는지 기록한다
        res["duplicate_inserts"] = c.query("""
            select toString(inserted_at), kafka_partition, min(kafka_offset), max(kafka_offset), count()
            from events where event_id in (select event_id from events group by event_id having count() > 1)
            group by inserted_at, kafka_partition order by inserted_at, kafka_partition""").result_rows[:60]
    if engine.startswith("Replacing"):
        res["rows_final"] = c.query("select count() from events final").result_rows[0][0]
        q = "select toStartOfHour(event_time) h, count() from events {f} group by h order by h"
        res["hourly_query_ms"] = {"plain": timed(c, q.format(f="")), "final": timed(c, q.format(f="final"))}
        c.command("OPTIMIZE TABLE events FINAL")
        res["rows_after_optimize"] = c.query("select count() from events").result_rows[0][0]
    return res


# 수집기 기본값이 --resume-from-table 로 바뀌었으므로(실험 B 결과), 비교 변형은 명시적으로 끈다.
for _k, _v in VARIANTS.items():
    if "--resume-from-table" not in _v[2]:
        _v[2].append("--no-resume-from-table")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=os.path.join(ROOT, "data", "day1.tsv"))
    ap.add_argument("--kills", type=int, default=15)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--only", nargs="*")
    ap.add_argument("--mode", choices=["random", "failpoint"], default="failpoint")
    ap.add_argument("--fp-prob", type=float, default=0.1)
    a = ap.parse_args()
    tr = truth(a.data)
    for name, (engine, ts, args) in VARIANTS.items():
        if a.only and name not in a.only:
            continue
        r = run(name, engine, ts, args, a.data, tr, a.kills, a.seed, a.mode, a.fp_prob)
        r["env"] = {"cpus": os.cpu_count(), "seed": a.seed, "fp_prob": a.fp_prob}
        save(("randomkill_" if a.mode == "random" else "failpoint_") + name, r)
        print(r, flush=True)

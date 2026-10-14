"""실험 A 보조: 쌓인 백로그(day1 전체)를 비우는 최대 처리량."""
import time
from common import save, spawn, truth, ROOT
from slp.infra import ch, create_events_table, reset_topics
import os

data = os.path.join(ROOT, "data", "day1.tsv")
tr = truth(data)
out = {}
for name, args in {"batch_5000_1s": ["--batch-size", "5000", "--batch-ms", "1000"],
                   "batch_50000_1s": ["--batch-size", "50000", "--batch-ms", "1000"]}.items():
    c = ch(); reset_topics(); create_events_table(c, "events")
    spawn("slp.produce", data).wait()
    t0 = time.time()
    p = spawn("slp.consume", "--table", "events", "--group", f"cap-{name}-{int(t0)}", *args, "--idle-exit", "3")
    while c.query("select count() from events").result_rows[0][0] < tr["valid"]:
        time.sleep(0.2)
    dt = time.time() - t0
    p.wait()
    out[name] = {"rows": tr["valid"], "seconds": round(dt, 1), "rows_per_s": round(tr["valid"] / dt)}
    print(name, out[name], flush=True)
save("a_capacity", {"truth_valid": tr["valid"], "cpus": os.cpu_count(), "results": out,
                    "note": "소비자 시작~전체 적재 완료. 소비자 기동·그룹 합류 시간 포함"})

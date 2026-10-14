"""실험 B 부가 관찰: kill -9 후 재시작한 수집기가 첫 메시지를 받기까지 걸리는 시간."""
import os
import re
import time

from common import ROOT, save, spawn

from slp.infra import ch, create_events_table, reset_topics

DATA = os.path.join(ROOT, "data", "day1.tsv")


def first_msg(log, timeout=90):
    t = time.time()
    while time.time() - t < timeout:
        m = re.search(r"FIRST_MSG ([\d.]+)s", open(log).read())
        if m:
            return float(m.group(1))
        time.sleep(0.2)
    return None


out = {}
for name, args in {"default_session_45s": [],
                   "session_6s": ["--session-timeout-ms", "6000"],
                   "static_membership": ["--instance-id", "ingest-0"]}.items():
    reset_topics(); create_events_table(ch(), "events")
    spawn("slp.produce", DATA).wait()
    group = f"r-{name}-{int(time.time())}"
    trials = []
    for i in range(3):
        la, lb = (os.path.join(ROOT, "data", f"r_{name}_{i}_{x}.log") for x in "ab")
        a = spawn("slp.consume", "--group", group, *args, log=la)
        first_msg(la)
        time.sleep(1)
        a.kill(); a.wait()
        b = spawn("slp.consume", "--group", group, *args, log=lb)
        trials.append(first_msg(lb))
        b.kill(); b.wait()
        time.sleep(1)
    out[name] = {"restart_to_first_msg_s": trials}
    print(name, out[name], flush=True)
save("b_restart_stall", {"note": "재시작 프로세스 기동부터 첫 메시지 수신까지. 매 시도 직전 같은 그룹 소비자를 SIGKILL",
                         "results": out, "cpus": os.cpu_count()})

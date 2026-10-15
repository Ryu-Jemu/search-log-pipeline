"""문서에 쓴 환경 사실 몇 가지를 다시 확인해 원자료로 남긴다(독립 검증에서 '기록 없음'으로 지적된 항목).

1. clickhouse-connect가 시간대 없는 datetime을 INSERT할 때 어떻게 해석하는가(호스트 시간대 함께 기록)
2. Airflow 3 설정 create_cron_data_intervals 값과, 실제 DAG run의 data interval 길이
3. abuse_detection DAG를 `airflow dags test`로 한 구간 실행한 결과(판정 줄과 run 최종 상태)
"""
import json
import os
import subprocess
import time
from datetime import datetime, timezone

from c2_metrics_dag import COMPOSE, psql
from common import save

from slp.infra import ch


def tz_roundtrip() -> dict:
    c = ch()
    c.command("DROP TABLE IF EXISTS tz_check SYNC")
    c.command("CREATE TABLE tz_check (k String, t DateTime64(3, 'UTC')) ENGINE = Memory")
    naive = datetime(2026, 1, 5, 0, 58, 12)
    aware = naive.replace(tzinfo=timezone.utc)
    c.insert("tz_check", [["naive", naive], ["aware_utc", aware]], column_names=["k", "t"])
    rows = {k: v for k, v in c.query("SELECT k, toString(t) FROM tz_check").result_rows}
    c.command("DROP TABLE tz_check SYNC")
    return {"host_tz": time.strftime("%Z %z"), "inserted": naive.isoformat(), "stored_as_utc": rows,
            "note": "같은 벽시계 값 2026-01-05 00:58:12를 시간대 없이 / UTC로 명시해 INSERT한 뒤 서버 문자열로 읽음"}


def airflow_intervals() -> dict:
    cfg = subprocess.run(COMPOSE + ["exec", "-T", "airflow", "airflow", "config", "get-value", "scheduler",
                                    "create_cron_data_intervals"], capture_output=True, text=True).stdout.strip()
    runs = psql("""select dag_id, count(*), min(extract(epoch from data_interval_end - data_interval_start)),
                          max(extract(epoch from data_interval_end - data_interval_start)),
                          min(data_interval_start), max(data_interval_start)
                   from dag_run where run_type = 'scheduled' group by dag_id order by dag_id""")
    return {"create_cron_data_intervals": cfg.splitlines()[-1] if cfg else None,
            "scheduled_runs": [{"dag_id": r[0], "runs": int(r[1]), "interval_s_min": float(r[2]),
                                "interval_s_max": float(r[3]), "first_start": r[4], "last_start": r[5]}
                               for r in runs]}


def abuse_dag_test(logical: str = "2026-01-05T08:00:00+00:00") -> dict:
    out = subprocess.run(COMPOSE + ["exec", "-T", "airflow", "airflow", "dags", "test", "abuse_detection", logical],
                         capture_output=True, text=True, timeout=600).stdout
    keep = [line.strip()[:300] for line in out.splitlines()
            if "ABUSE_DETECTED" in line or "탐지 없음" in line or "DagRun Finished" in line]
    return {"logical_date": logical, "lines": keep,
            "note": "수동 run은 data interval이 logical date에서 끝나는 1시간([07:00, 08:00))으로 정해진다. "
                    "대상은 그 시점의 slp.events(마지막 관측 실행의 day1 00~13:10 적재본)"}


if __name__ == "__main__":
    res = {"taken_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
           "clickhouse_connect_naive_datetime": tz_roundtrip(), "airflow": airflow_intervals(),
           "abuse_dag_test": abuse_dag_test(),
           "note": "airflow 항목은 마지막 관측 실행(d_observe.py) 뒤의 메타DB 상태"}
    print(json.dumps(res, ensure_ascii=False, indent=1))
    print(save("fact_checks_mac_m4", res, overwrite=True))  # 같은 확인을 항목을 늘려 다시 실행

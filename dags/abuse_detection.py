"""시간별 어뷰징 탐지 DAG: 시간 H의 데이터가 확정 조건(워터마크 ≥ H 끝 + 15분)을 넘으면 규칙 R1~R4를 평가한다.

- 지표 DAG(hourly_metrics)와 분리했다. 이유:
  · 실패·재실행 단위가 다르다. 탐지 임계값을 바꿔 다시 돌려도 지표 버전이 바뀌지 않는다.
  · 탐지 실패가 지표 확정을 막지 않는다(반대도 마찬가지).
- 확정 조건과 같은 +15분을 기다리는 이유: 늦은 도착이 거의 다 들어온 뒤 한 번만 평가해 결과가 바뀌지 않게 한다.
  대신 탐지가 시간 끝 + 15분 이후에 나온다(실시간 차단 용도가 아니다).
- 규칙과 임계값은 slp.abuse(임계값은 day1에서만 결정, docs/03-abuse-detection.md).
"""
from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime, timedelta, timezone

from airflow.sdk import CronDataIntervalTimetable, PokeReturnValue, dag, get_current_context, task

from slp import abuse, metrics
from slp.infra import ch

log = logging.getLogger("slp.abuse_detection")
START = datetime(2026, 1, 5, tzinfo=timezone.utc)
HOURS = 12


def _hour() -> datetime:
    return datetime.fromtimestamp(get_current_context()["data_interval_start"].timestamp(), timezone.utc)


@task.sensor(task_id="wait_watermark_15m", poke_interval=1, timeout=900, mode="poke", retries=0)
def wait_final() -> PokeReturnValue:
    target = _hour() + timedelta(hours=1, minutes=15)
    return PokeReturnValue(is_done=metrics.watermark_reached(ch(), target), xcom_value=target.isoformat())


@task(retries=1, retry_delay=timedelta(seconds=2))
def detect() -> dict:
    c = ch()
    abuse.create_detections_table(c)
    hour = _hour()
    rows = abuse.detect_hour(c, "events", hour)
    abuse.write_detections(c, hour, rows)
    counts = dict(Counter(r["rule"] for r in rows))
    if rows:
        log.warning("ABUSE_DETECTED hour=%s %s", hour, counts)
    else:
        log.info("hour=%s 탐지 없음", hour)
    return {"hour": str(hour), **counts}


@dag(dag_id="abuse_detection", schedule=CronDataIntervalTimetable("0 * * * *", timezone="UTC"),
     start_date=START, end_date=START + timedelta(hours=HOURS - 1), catchup=True, max_active_runs=3,
     tags=["slp"], doc_md=__doc__)
def abuse_detection():
    wait_final() >> detect()


abuse_detection()

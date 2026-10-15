"""시간별 검색 지표 DAG: 워터마크 기반 잠정(v1) → 확정(v2) → 확정 후 늦은 도착 검사·재확정.

- 시간 H = 이 run의 data interval [H, H+1h). CronDataIntervalTimetable을 명시한다
  (Airflow 3의 기본 cron 시간표는 data interval 길이가 0이다).
- 재생이 배속이라 벽시계로 기다리지 않는다. 각 단계는 ClickHouse 워터마크(slp.metrics)가
  H 끝 + 1분 / 15분 / 60분(가상 시각)을 넘었을 때만 진행한다.
- 가상 날짜(2026-01-05)가 과거이므로 catchup으로 run이 바로 만들어지고, 진행은 데이터가 결정한다.
- 근거: docs/troubleshooting/C-late-events.md(+1분 처리 지연 흡수, +15분 최대 지연 599초에 여유).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow.sdk import CronDataIntervalTimetable, PokeReturnValue, dag, get_current_context, task

from slp import metrics
from slp.infra import ch

log = logging.getLogger("slp.hourly_metrics")
START = datetime(2026, 1, 5, tzinfo=timezone.utc)  # 재생기 가상 시계 시작
HOURS = 12                                          # 00~12시 (실험 C와 같은 범위)


def _hour() -> datetime:
    start = get_current_context()["data_interval_start"]
    return datetime.fromtimestamp(start.timestamp(), timezone.utc)


def wait_watermark(minutes: int):
    @task.sensor(task_id=f"wait_watermark_{minutes}m", poke_interval=1, timeout=900, mode="poke", retries=0)
    def _wait() -> PokeReturnValue:
        target = _hour() + timedelta(hours=1, minutes=minutes)
        return PokeReturnValue(is_done=metrics.watermark_reached(ch(), target), xcom_value=target.isoformat())
    return _wait()


@task(retries=1, retry_delay=timedelta(seconds=2))
def compute(version: int, stage: str, is_final: int) -> dict:
    c = ch()
    metrics.create_tables(c)  # IF NOT EXISTS
    r = metrics.compute_hour(c, _hour(), version, stage, is_final)
    log.info("%s v%d %s: searches=%d clicks=%d users=%d zero_click=%d watermark=%s", r["hour"], version, stage,
             r["searches"], r["clicks"], r["unique_users"], r["zero_click_searches"], r["watermark"])
    return {k: str(v) for k, v in r.items()}


# 재시도하지 않는다: 재확정(v3) 뒤 기록 직전에 실패하면 재시도가 v3 기준으로 다시 세어 늦은 도착 0건을 남긴다.
# 실패하면 태스크를 수동으로 다시 실행하고, 첫 시도의 경고 로그를 함께 확인한다.
@task(retries=0)
def late_after_final_check() -> dict:
    r = metrics.late_after_final(ch(), _hour())
    if r["late_events"]:
        # 경보: 허용 지연(+15분)을 넘겨 확정 뒤에 들어온 이벤트. 재확정 버전을 함께 남긴다.
        log.warning("LATE_AFTER_FINAL hour=%s late_events=%d (inserted_at 기준 %d) final_v%d → refinal_v%d",
                    r["hour"], r["late_events"], r["late_by_inserted_at"], r["final_version"],
                    r["refinalized_version"])
    else:
        log.info("hour=%s 확정 후 늦은 도착 없음", r["hour"])
    return {k: str(v) for k, v in r.items() if k != "refinalized"}


@dag(dag_id="hourly_metrics", schedule=CronDataIntervalTimetable("0 * * * *", timezone="UTC"),
     start_date=START, end_date=START + timedelta(hours=HOURS - 1), catchup=True, max_active_runs=3,
     tags=["slp"], doc_md=__doc__)
def hourly_metrics():
    (wait_watermark(1) >> compute.override(task_id="provisional")(1, "provisional", 0)
     >> wait_watermark(15) >> compute.override(task_id="finalize")(2, "final", 1)
     >> wait_watermark(60) >> late_after_final_check())


hourly_metrics()

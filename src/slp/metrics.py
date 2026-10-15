"""시간별 지표: 워터마크 판정, 버전별 집계(잠정 v1 → 확정 v2 → 재확정 v3…), 확정 후 늦은 도착 검사.

Airflow DAG(dags/hourly_metrics.py)·실험·시험이 같은 함수를 쓴다(SQL은 이 파일에만 둔다).

워터마크
  파티션별 max(event_time) 중 최솟값. 판정은 같은 뜻의 더 싼 형태
  "모든 Kafka 파티션에 event_time >= target 인 이벤트가 적재돼 있는가"로 한다.
  - 한 파티션에 미래 시각 이상값이 들어와도 다른 파티션이 따라오기 전에는 워터마크가 움직이지 않는다.
  - 데이터가 없는 파티션이 있으면 판정이 거짓이다(그 파티션을 빼고 최솟값을 구해 앞서 나가는 문제를 막는다).
  - 한 파티션 안에서는 오프셋 순서대로 적재되므로(수집기는 파티션마다 하나, 적재 후 커밋),
    target 이후 이벤트가 보이면 그보다 먼저 Kafka에 들어온 같은 파티션의 이벤트도 모두 보인다.

확정 후 늦은 도착
  확정 집계와 같은 SELECT에서 파티션별 최대 kafka_offset(maxMap)을 경계로 저장한다.
  나중에 "경계 밖 오프셋을 가진 그 시간 이벤트"를 세면 확정 뒤에 적재된 이벤트 수가 된다.
  inserted_at 비교와 달리 시계·동시 적재의 영향을 받지 않는다(실험 B의 오프셋 재개와 같은 전제).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from slp.infra import N_PARTITIONS

TOP_N = 10
CLICK_WINDOW_MIN = 5  # zero_click 판정: 검색 시간대 끝 + 5분까지의 클릭을 본다

DDL = [
    """CREATE TABLE IF NOT EXISTS {name} (
         hour DateTime('UTC'), version UInt32, is_final UInt8, stage LowCardinality(String),
         computed_at DateTime64(3, 'UTC'), watermark Nullable(DateTime64(3, 'UTC')),
         searches UInt64, clicks UInt64, unique_users UInt64, zero_click_searches UInt64,
         offset_boundary Map(UInt16, UInt64), max_inserted_at DateTime64(3, 'UTC'))
       ENGINE = {engine}""",
]
METRICS = "hourly_metrics"            # 서빙용: 시간당 최신 버전 1행(ReplacingMergeTree)
VERSIONS = "hourly_metrics_versions"  # 이력: 모든 버전(병합돼도 v1·v2가 남는다)
TOP = "hourly_top_queries"
LATE = "hourly_late_checks"
COLUMNS = ["hour", "version", "is_final", "stage", "computed_at", "watermark", "searches", "clicks",
           "unique_users", "zero_click_searches", "offset_boundary", "max_inserted_at"]


def create_tables(client, drop: bool = False) -> None:
    if drop:
        for t in (f"{METRICS}_latest", METRICS, VERSIONS, TOP, LATE):
            client.command(f"DROP TABLE IF EXISTS {t} SYNC")
    client.command(DDL[0].format(name=METRICS, engine="ReplacingMergeTree(version) ORDER BY hour"))
    # 태스크 재시도로 같은 (hour, version)이 다시 들어와도 병합 시 1행만 남도록 ReplacingMergeTree를 쓴다.
    client.command(DDL[0].format(name=VERSIONS, engine="ReplacingMergeTree(computed_at) ORDER BY (hour, version)"))
    client.command(f"""CREATE TABLE IF NOT EXISTS {TOP} (hour DateTime('UTC'), rank UInt8, version UInt32,
                        query_norm String, searches UInt64)
                       ENGINE = ReplacingMergeTree(version) ORDER BY (hour, rank)""")
    client.command(f"""CREATE TABLE IF NOT EXISTS {LATE} (hour DateTime('UTC'), checked_at DateTime64(3, 'UTC'),
                        final_version UInt32, late_events UInt64, late_by_inserted_at UInt64,
                        refinalized_version UInt32, watermark Nullable(DateTime64(3, 'UTC')))
                       ENGINE = MergeTree ORDER BY (hour, checked_at)""")
    # 읽는 쪽은 이 뷰를 쓴다(FINAL을 잊지 않도록). CTR은 재생기 클릭 확률 60% 가정에서 나온 값이다.
    client.command(f"""CREATE VIEW IF NOT EXISTS {METRICS}_latest AS
                       SELECT *, if(searches = 0, 0, clicks / searches) AS ctr FROM {METRICS} FINAL""")


def _utc(t: datetime) -> datetime:
    """시간대가 없는 값은 UTC로 본다.
    clickhouse-connect는 DateTime64('UTC') 값을 시간대 없는 UTC 시각으로 돌려주지만, 시간대 없는 값을 INSERT하면
    호스트 로컬 시간으로 해석한다(KST 호스트에서 9시간 어긋남을 실측). 그래서 쓰기 전에 항상 UTC를 명시한다."""
    return t.replace(tzinfo=timezone.utc) if t.tzinfo is None else t.astimezone(timezone.utc)


def _ts(t: datetime) -> str:
    return f"toDateTime64('{_utc(t).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}', 3, 'UTC')"


def _hour_range(hour: datetime) -> str:
    return f"event_time >= {_ts(hour)} AND event_time < {_ts(hour + timedelta(hours=1))}"


def _visible(boundary: dict[int, int]) -> str:
    """경계(파티션 → 최대 오프셋) 안에 있는 행 조건. 경계에 없는 파티션의 행은 모두 경계 밖이다."""
    if not boundary:
        return "0"
    return "(" + " OR ".join(f"(kafka_partition = {int(p)} AND kafka_offset <= {int(o)})"
                             for p, o in sorted(boundary.items())) + ")"


def _missing_table(e: Exception) -> bool:
    return "UNKNOWN_TABLE" in str(e) or "Code: 60." in str(e)


def watermark_reached(client, target: datetime, table: str = "events", partitions: int = N_PARTITIONS) -> bool:
    try:
        n = client.query(f"SELECT uniqExact(kafka_partition) FROM {table} WHERE event_time >= {_ts(target)}"
                         ).result_rows[0][0]
    except Exception as e:  # noqa: BLE001 - 표가 아직 없으면 '아직 아님'
        if _missing_table(e):
            return False
        raise
    return n >= partitions


def watermark(client, since: datetime | None = None, table: str = "events",
              partitions: int = N_PARTITIONS) -> datetime | None:
    """기록용 워터마크 값. since 이후 이벤트가 없는 파티션이 있으면 None."""
    where = f"WHERE event_time >= {_ts(since)}" if since else ""
    try:
        wm, n = client.query(f"""SELECT min(m), count() FROM (
            SELECT kafka_partition, max(event_time) m FROM {table} {where} GROUP BY kafka_partition)""").result_rows[0]
    except Exception as e:  # noqa: BLE001
        if _missing_table(e):
            return None
        raise
    return _utc(wm) if n >= partitions else None


def aggregate(client, hour: datetime, table: str = "events", partitions: int = N_PARTITIONS) -> dict:
    """시간 [hour, hour+1h)의 지표를 계산만 한다(기록하지 않음). 경계(오프셋)는 같은 SELECT에서 잡는다.
    재생이 끝난 뒤 호출하면 그대로 정답(최종 적재본 기준)이 된다."""
    hour = _utc(hour)
    rng = _hour_range(hour)
    s, k, u, (parts, offs), max_ins, now = client.query(f"""
        SELECT countIf(event_type = 'search'), countIf(event_type = 'click'), uniqExact(user_id),
               maxMap([kafka_partition], [kafka_offset]), max(inserted_at), now64(3)
        FROM {table} WHERE {rng}""").result_rows[0]
    boundary = dict(zip(parts, offs))
    vis = _visible(boundary)
    end_click = hour + timedelta(hours=1, minutes=CLICK_WINDOW_MIN)
    # 같은 경계 안의 검색만 본다. 클릭 하위쿼리에는 경계를 걸지 않아 완전한 스냅숏은 아니다(docs/02 §8).
    zero = client.query(f"""
        SELECT count() FROM {table}
        WHERE event_type = 'search' AND {rng} AND {vis}
          AND (session_id, qid) NOT IN (
              SELECT session_id, qid FROM {table}
              WHERE event_type = 'click' AND event_time >= {_ts(hour)} AND event_time < {_ts(end_click)})"""
                        ).result_rows[0][0]
    top = client.query(f"""
        SELECT query_norm, count() c FROM {table} WHERE event_type = 'search' AND {rng} AND {vis}
        GROUP BY query_norm ORDER BY c DESC, query_norm LIMIT {TOP_N}""").result_rows
    top += [("", 0)] * (TOP_N - len(top))  # 항상 N행을 써서 이전 버전 행이 남지 않게 한다
    return {"hour": hour, "computed_at": _utc(now),
            "watermark": watermark(client, since=hour, table=table, partitions=partitions), "searches": s,
            "clicks": k, "unique_users": u, "zero_click_searches": zero, "offset_boundary": boundary,
            "max_inserted_at": _utc(max_ins), "top": top}


def compute_hour(client, hour: datetime, version: int, stage: str, is_final: int,
                 table: str = "events", partitions: int = N_PARTITIONS) -> dict:
    """aggregate 결과를 버전 행으로 기록한다(이력·서빙·상위 쿼리 표)."""
    row = {**aggregate(client, hour, table, partitions), "version": version, "is_final": is_final, "stage": stage}
    values = [[row[c] for c in COLUMNS]]
    client.insert(VERSIONS, values, column_names=COLUMNS)
    client.insert(METRICS, values, column_names=COLUMNS)
    client.insert(TOP, [[row["hour"], i + 1, version, q, c] for i, (q, c) in enumerate(row["top"])],
                  column_names=["hour", "rank", "version", "query_norm", "searches"])
    return row


def latest_final(client, hour: datetime) -> tuple | None:
    hour = _utc(hour)
    r = client.query(f"""SELECT version, offset_boundary, max_inserted_at FROM {VERSIONS}
                         WHERE hour = toDateTime('{hour:%Y-%m-%d %H:%M:%S}', 'UTC') AND is_final = 1
                         ORDER BY version DESC LIMIT 1""").result_rows
    return r[0] if r else None


def late_after_final(client, hour: datetime, table: str = "events", refinalize: bool = True,
                     partitions: int = N_PARTITIONS) -> dict:
    """확정 뒤 적재된 그 시간 이벤트 수를 세고, 0보다 크면 재확정(max(version)+1)한다."""
    hour = _utc(hour)
    fin = latest_final(client, hour)
    if fin is None:
        raise RuntimeError(f"{hour} 확정본이 없다")
    version, boundary, max_ins = fin
    late, late_ins = client.query(f"""
        SELECT countIf(NOT {_visible(boundary)}), countIf(inserted_at > {_ts(max_ins)})
        FROM {table} WHERE {_hour_range(hour)}""").result_rows[0]
    out = {"hour": hour, "final_version": version, "late_events": late, "late_by_inserted_at": late_ins,
           "refinalized_version": 0, "watermark": watermark(client, since=hour, table=table, partitions=partitions)}
    if late > 0 and refinalize:
        top_v = client.query(f"""SELECT max(version) FROM {VERSIONS}
                                 WHERE hour = toDateTime('{hour:%Y-%m-%d %H:%M:%S}', 'UTC')""").result_rows[0][0]
        out["refinalized_version"] = top_v + 1
        out["refinalized"] = compute_hour(client, hour, top_v + 1, "refinal", 1, table=table, partitions=partitions)
    now = _utc(client.query("SELECT now64(3)").result_rows[0][0])
    client.insert(LATE, [[hour, now, version, late, late_ins, out["refinalized_version"], out["watermark"]]],
                  column_names=["hour", "checked_at", "final_version", "late_events", "late_by_inserted_at",
                                "refinalized_version", "watermark"])
    return out

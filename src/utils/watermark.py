"""
Watermark 유틸리티 — 파이프라인/유지보수 진행 상태 추적

- CDC watermark: CDC 처리 진행 기록 (append/merge)
- Maintenance watermark: 유지보수 프로시저 실행 이력
- 테이블 생성, 조회, 정리(purge) 포함
"""

from datetime import UTC, datetime

from pyspark.sql import SparkSession

from utils.spark_logging import SparkLoggerManager

# ---------------------------------------------------------------------------
# Table Setup
# ---------------------------------------------------------------------------


def ensure_watermark_tables(spark: SparkSession, catalog: str, warehouse: str) -> None:
    """ops_bronze의 CDC/Maintenance watermark 테이블을 모두 초기화한다."""
    _ensure_cdc_watermark_table(spark, catalog, warehouse)
    _ensure_maintenance_watermark_table(spark, catalog, warehouse)


def _ensure_cdc_watermark_table(spark: SparkSession, catalog: str, warehouse: str) -> None:
    """ops_bronze.cdc_watermark 테이블이 없으면 생성한다."""
    logger = SparkLoggerManager().get_logger()
    full_table_name = f"{catalog}.ops_bronze.cdc_watermark"
    if not spark.catalog.tableExists(full_table_name):
        logger.info(f"Creating CDC watermark table: {full_table_name}")
        spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS {catalog}.ops_bronze
            LOCATION '{warehouse}/ops_bronze'
        """)
        spark.sql(f"""
            CREATE TABLE {full_table_name} (
                dag_id                  STRING,
                bronze_schema           STRING,
                table_name              STRING,
                scheduled_at            TIMESTAMP,
                max_event_ts            TIMESTAMP,
                processed_at            TIMESTAMP,
                min_offset              BIGINT,
                max_offset              BIGINT,
                event_count             BIGINT,
                processing_duration_sec DOUBLE,
                batch_id                BIGINT
            ) USING iceberg
            LOCATION '{warehouse}/ops_bronze/cdc_watermark'
            TBLPROPERTIES (
                'format-version' = '2',
                'write.metadata.delete-after-commit.enabled' = 'true',
                'write.metadata.previous-versions-max' = '5'
            )
        """)


def _ensure_maintenance_watermark_table(spark: SparkSession, catalog: str, warehouse: str) -> None:
    """ops_bronze.maintenance_watermark 테이블이 없으면 생성한다."""
    logger = SparkLoggerManager().get_logger()
    full_table_name = f"{catalog}.ops_bronze.maintenance_watermark"
    if not spark.catalog.tableExists(full_table_name):
        logger.info(f"Creating maintenance watermark table: {full_table_name}")
        spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS {catalog}.ops_bronze
            LOCATION '{warehouse}/ops_bronze'
        """)
        spark.sql(f"""
            CREATE TABLE {full_table_name} (
                dag_id                  STRING      COMMENT 'Airflow DAG ID (실행 주체)',
                bronze_schema           STRING      COMMENT '대상 스키마',
                table_name              STRING      COMMENT '대상 테이블',
                procedure_type          STRING      COMMENT '프로시저 유형',
                started_at              TIMESTAMP   COMMENT '프로시저 실행 시작 시각',
                completed_at            TIMESTAMP   COMMENT '프로시저 실행 완료 시각 (실패 시 NULL)',
                duration_sec            DOUBLE      COMMENT '실행 소요 시간 (초)',
                status                  STRING      COMMENT 'success, failed, skipped',
                error_message           STRING      COMMENT '실패 시 에러 메시지',
                rewritten_files_count   BIGINT      COMMENT '재작성된 파일 수',
                added_files_count       BIGINT      COMMENT '새로 추가된 파일 수',
                batch_id                BIGINT      COMMENT 'CDC batch_id'
            ) USING iceberg
            LOCATION '{warehouse}/ops_bronze/maintenance_watermark'
            TBLPROPERTIES (
                'format-version' = '2',
                'write.metadata.delete-after-commit.enabled' = 'true',
                'write.metadata.previous-versions-max' = '5'
            )
        """)


# ---------------------------------------------------------------------------
# CDC Watermark Write
# ---------------------------------------------------------------------------


def _build_cdc_values(
    dag_id: str,
    bronze_schema: str,
    table_name: str,
    event_count: int,
    max_event_ts,
    min_offset: int | None,
    max_offset: int | None,
    batch_id: int | None,
    processing_duration_sec: float | None,
    scheduled_at: str | None = None,
) -> str:
    """CDC watermark INSERT/MERGE에 사용할 SELECT 절을 생성한다."""
    max_event_ts_expr = f"TIMESTAMP '{max_event_ts}'" if max_event_ts else "CAST(NULL AS TIMESTAMP)"
    min_offset_expr = str(min_offset) if min_offset is not None else "CAST(NULL AS BIGINT)"
    max_offset_expr = str(max_offset) if max_offset is not None else "CAST(NULL AS BIGINT)"
    batch_id_expr = str(batch_id) if batch_id is not None else "CAST(NULL AS BIGINT)"
    duration_expr = str(processing_duration_sec) if processing_duration_sec is not None else "CAST(NULL AS DOUBLE)"
    scheduled_at_expr = f"TIMESTAMP '{scheduled_at}'" if scheduled_at else "CAST(NULL AS TIMESTAMP)"
    return f"""
        SELECT
            '{dag_id}' AS dag_id,
            '{bronze_schema}' AS bronze_schema,
            '{table_name}' AS table_name,
            {scheduled_at_expr} AS scheduled_at,
            {max_event_ts_expr} AS max_event_ts,
            current_timestamp() AS processed_at,
            {min_offset_expr} AS min_offset,
            {max_offset_expr} AS max_offset,
            {event_count} AS event_count,
            {duration_expr} AS processing_duration_sec,
            {batch_id_expr} AS batch_id
    """


def _log_cdc(
    bronze_schema: str,
    table_name: str,
    event_count: int,
    max_event_ts,
    min_offset: int | None,
    max_offset: int | None,
    processing_duration_sec: float | None,
) -> None:
    logger = SparkLoggerManager().get_logger()
    if processing_duration_sec is not None:
        logger.info(
            f"watermark: {bronze_schema}.{table_name}, "
            f"events={event_count}, max_ts={max_event_ts}, "
            f"offsets=[{min_offset}, {max_offset}], duration={processing_duration_sec:.1f}s"
        )
    else:
        logger.info(f"watermark: {bronze_schema}.{table_name}, events={event_count}, max_ts={max_event_ts}")


def append_cdc_watermark(
    spark: SparkSession,
    catalog: str,
    dag_id: str,
    bronze_schema: str,
    table_name: str,
    event_count: int,
    max_event_ts,
    min_offset: int | None = None,
    max_offset: int | None = None,
    batch_id: int | None = None,
    processing_duration_sec: float | None = None,
    scheduled_at: str | None = None,
) -> None:
    """cdc_watermark 테이블에 CDC 처리 기록을 append한다.

    동시 쓰기 시 Iceberg 스냅샷 충돌이 발생하지 않는다.
    최신 상태 조회 시 (dag_id, bronze_schema, table_name) 기준
    processed_at DESC로 첫 번째 행을 사용한다.
    """
    values = _build_cdc_values(
        dag_id,
        bronze_schema,
        table_name,
        event_count,
        max_event_ts,
        min_offset,
        max_offset,
        batch_id,
        processing_duration_sec,
        scheduled_at,
    )
    spark.sql(f"INSERT INTO {catalog}.ops_bronze.cdc_watermark {values}")
    _log_cdc(bronze_schema, table_name, event_count, max_event_ts, min_offset, max_offset, processing_duration_sec)


def merge_cdc_watermark(
    spark: SparkSession,
    catalog: str,
    dag_id: str,
    bronze_schema: str,
    table_name: str,
    event_count: int,
    max_event_ts,
    min_offset: int | None = None,
    max_offset: int | None = None,
    batch_id: int | None = None,
    processing_duration_sec: float | None = None,
    scheduled_at: str | None = None,
) -> None:
    """cdc_watermark 테이블에 CDC 처리 기록을 upsert한다.

    단일 writer 환경에서만 사용한다. 동시 쓰기 시 Iceberg 스냅샷 충돌이
    발생할 수 있으므로, 멀티스레드/멀티프로세스 환경에서는 append_cdc_watermark를 사용한다.
    """
    full_table = f"{catalog}.ops_bronze.cdc_watermark"
    values = _build_cdc_values(
        dag_id,
        bronze_schema,
        table_name,
        event_count,
        max_event_ts,
        min_offset,
        max_offset,
        batch_id,
        processing_duration_sec,
        scheduled_at,
    )
    spark.sql(f"""
        MERGE INTO {full_table} t
        USING ({values}) s
        ON t.dag_id = s.dag_id
           AND t.bronze_schema = s.bronze_schema
           AND t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET
            t.scheduled_at = s.scheduled_at,
            t.max_event_ts = s.max_event_ts,
            t.processed_at = s.processed_at,
            t.min_offset = s.min_offset,
            t.max_offset = s.max_offset,
            t.event_count = s.event_count,
            t.processing_duration_sec = s.processing_duration_sec,
            t.batch_id = s.batch_id
        WHEN NOT MATCHED THEN INSERT *
    """)
    _log_cdc(bronze_schema, table_name, event_count, max_event_ts, min_offset, max_offset, processing_duration_sec)


# ---------------------------------------------------------------------------
# Maintenance Watermark Write
# ---------------------------------------------------------------------------


def _escape_sql(s: str) -> str:
    """SQL 문자열 리터럴 내 싱글 쿼트를 이스케이프한다."""
    return s.replace("'", "''") if s else ""


def _build_maintenance_values(
    dag_id: str,
    bronze_schema: str,
    table_name: str,
    procedure_type: str,
    started_at: datetime,
    completed_at: datetime | None,
    duration_sec: float,
    status: str,
    error_message: str | None,
    rewritten_files_count: int | None,
    added_files_count: int | None,
    batch_id: int | None,
) -> str:
    """Maintenance watermark INSERT에 사용할 SELECT 절을 생성한다."""
    started_ts = started_at.strftime("%Y-%m-%d %H:%M:%S.%f")
    completed_ts = (
        f"TIMESTAMP '{completed_at.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
        if completed_at is not None
        else "CAST(NULL AS TIMESTAMP)"
    )
    error_expr = f"'{_escape_sql(error_message)}'" if error_message else "CAST(NULL AS STRING)"
    rewritten_expr = str(rewritten_files_count) if rewritten_files_count is not None else "CAST(NULL AS BIGINT)"
    added_expr = str(added_files_count) if added_files_count is not None else "CAST(NULL AS BIGINT)"
    batch_expr = str(batch_id) if batch_id is not None else "CAST(NULL AS BIGINT)"

    return f"""
        SELECT
            '{dag_id}' AS dag_id,
            '{bronze_schema}' AS bronze_schema,
            '{table_name}' AS table_name,
            '{procedure_type}' AS procedure_type,
            TIMESTAMP '{started_ts}' AS started_at,
            {completed_ts} AS completed_at,
            {duration_sec} AS duration_sec,
            '{status}' AS status,
            {error_expr} AS error_message,
            {rewritten_expr} AS rewritten_files_count,
            {added_expr} AS added_files_count,
            {batch_expr} AS batch_id
    """


def _log_maintenance(
    bronze_schema: str,
    table_name: str,
    procedure_type: str,
    status: str,
    duration_sec: float,
) -> None:
    logger = SparkLoggerManager().get_logger()
    logger.info(
        f"maintenance: {bronze_schema}.{table_name}, "
        f"procedure={procedure_type}, status={status}, duration={duration_sec:.1f}s"
    )


def append_maintenance_watermark(
    spark: SparkSession,
    catalog: str,
    dag_id: str,
    bronze_schema: str,
    table_name: str,
    procedure_type: str,
    started_at: datetime,
    completed_at: datetime | None,
    duration_sec: float,
    status: str,
    error_message: str | None = None,
    rewritten_files_count: int | None = None,
    added_files_count: int | None = None,
    batch_id: int | None = None,
) -> None:
    """maintenance_watermark 테이블에 프로시저 실행 이력을 append한다.

    Args:
        started_at: 프로시저 시작 wall-clock 시각 (datetime, UTC)
        completed_at: 프로시저 완료 wall-clock 시각 (실패/스킵 시 None)
        duration_sec: time.monotonic() 기반 소요 시간 (초)
    """
    values = _build_maintenance_values(
        dag_id,
        bronze_schema,
        table_name,
        procedure_type,
        started_at,
        completed_at,
        duration_sec,
        status,
        error_message,
        rewritten_files_count,
        added_files_count,
        batch_id,
    )
    spark.sql(f"INSERT INTO {catalog}.ops_bronze.maintenance_watermark {values}")
    _log_maintenance(bronze_schema, table_name, procedure_type, status, duration_sec)


# ---------------------------------------------------------------------------
# Query / Scheduling
# ---------------------------------------------------------------------------


def get_last_completed_map(
    spark: SparkSession,
    catalog: str,
    tables: list[str],
    procedure_type: str,
) -> dict[str, datetime | None]:
    """전체 테이블의 마지막 성공 시각을 한 번의 벌크 쿼리로 조회한다.

    Args:
        tables: ["bronze_schema.table_name", ...] 형태의 테이블 목록
    Returns:
        {"bronze_schema.table_name": datetime | None, ...}
    """
    full_table = f"{catalog}.ops_bronze.maintenance_watermark"
    rows = spark.sql(f"""
        SELECT bronze_schema, table_name, MAX(completed_at) AS last_completed
        FROM {full_table}
        WHERE procedure_type = '{procedure_type}'
          AND status = 'success'
        GROUP BY bronze_schema, table_name
    """).collect()

    result: dict[str, datetime | None] = {f"{row.bronze_schema}.{row.table_name}": row.last_completed for row in rows}
    for t in tables:
        if t not in result:
            result[t] = None
    return result


def should_run(last_completed: datetime | None, interval_seconds: int) -> bool:
    """마지막 성공 시각 기준으로 실행 여부를 판단한다."""
    if last_completed is None:
        return True
    if last_completed.tzinfo is None:
        last_completed = last_completed.replace(tzinfo=UTC)
    elapsed = (datetime.now(UTC) - last_completed).total_seconds()
    return elapsed >= interval_seconds


# ---------------------------------------------------------------------------
# Purge
# ---------------------------------------------------------------------------


def purge_watermarks(spark: SparkSession, catalog: str, retention_days: int = 14) -> None:
    """cdc_watermark와 maintenance_watermark에서 retention_days 이전 레코드를 삭제한다.

    각 테이블의 시간 컬럼이 다르므로 개별 쿼리를 사용한다:
    - cdc_watermark: processed_at 기준
    - maintenance_watermark: started_at 기준
    """
    logger = SparkLoggerManager().get_logger()

    purge_targets = [
        (f"{catalog}.ops_bronze.cdc_watermark", "processed_at"),
        (f"{catalog}.ops_bronze.maintenance_watermark", "started_at"),
    ]
    for table, time_column in purge_targets:
        try:
            if not spark.catalog.tableExists(table):
                continue
            logger.info(f"Purging records older than {retention_days} days from {table} (column={time_column})")
            spark.sql(f"""
                DELETE FROM {table}
                WHERE {time_column} < current_timestamp() - INTERVAL {retention_days} DAYS
            """)
            logger.info(f"Purge completed: {table}")
        except Exception as e:
            logger.warn(f"Purge failed for {table}: {e}")

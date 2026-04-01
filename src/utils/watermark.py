"""
CDC Watermark 쓰기 유틸리티

cdc_watermark 테이블에 CDC 처리 기록을 INSERT/MERGE하는 함수 제공.
테이블 생성·정리 등 유지보수 작업은 utils/maintenance.py 참조.
"""

from pyspark.sql import SparkSession

from utils.settings import Settings
from utils.spark_logging import SparkLoggerManager


def _build_watermark_values(
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
    """watermark INSERT/MERGE에 사용할 SELECT 절을 생성한다."""
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


def _log_watermark(
    bronze_schema, table_name, event_count, max_event_ts, min_offset, max_offset, processing_duration_sec
):
    logger = SparkLoggerManager().get_logger()
    if processing_duration_sec is not None:
        logger.info(
            f"watermark: {bronze_schema}.{table_name}, "
            f"events={event_count}, max_ts={max_event_ts}, "
            f"offsets=[{min_offset}, {max_offset}], duration={processing_duration_sec:.1f}s"
        )
    else:
        logger.info(f"watermark: {bronze_schema}.{table_name}, events={event_count}, max_ts={max_event_ts}")


def append_watermark(
    spark: SparkSession,
    settings: Settings,
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
    values = _build_watermark_values(
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
    spark.sql(f"INSERT INTO {settings.CATALOG}.ops_bronze.cdc_watermark {values}")
    _log_watermark(
        bronze_schema, table_name, event_count, max_event_ts, min_offset, max_offset, processing_duration_sec
    )


def merge_watermark(
    spark: SparkSession,
    settings: Settings,
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
    발생할 수 있으므로, 멀티스레드/멀티프로세스 환경에서는 append_watermark를 사용한다.
    """
    full_table = f"{settings.CATALOG}.ops_bronze.cdc_watermark"
    values = _build_watermark_values(
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
    _log_watermark(
        bronze_schema, table_name, event_count, max_event_ts, min_offset, max_offset, processing_duration_sec
    )

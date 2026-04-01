"""
Iceberg Maintenance Utilities — CDC 파이프라인과 별도 DAG에서 공유하는 유지보수 유틸리티.

주요 구성:
- ProcessedTableTracker: 스레드 안전한 수정 테이블 추적기
- maintenance_watermark 테이블 관리 (생성, 이력 기록, 벌크 조회)
- 프로시저 실행 함수 (compaction, position delete compaction, orphan cleanup)
- watermark 데이터 정리 (14일 이전 레코드 삭제)
"""

import threading
import time
from datetime import UTC, datetime

from pyspark.sql import SparkSession

from utils.spark_logging import SparkLoggerManager

# ---------------------------------------------------------------------------
# ProcessedTableTracker
# ---------------------------------------------------------------------------


class ProcessedTableTracker:
    """스레드 안전한 수정 테이블 추적기.

    멀티스레드 CDC 환경에서 비어있지 않은 배치를 처리한 테이블을 추적한다.
    """

    def __init__(self):
        self._tables: set[str] = set()
        self._lock = threading.Lock()

    def mark(self, full_table_name: str) -> None:
        with self._lock:
            self._tables.add(full_table_name)

    def get_and_clear(self) -> set[str]:
        with self._lock:
            tables = set(self._tables)
            self._tables.clear()
            return tables


# ---------------------------------------------------------------------------
# Watermark Table Management
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


def record_maintenance(
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
    """maintenance_watermark에 이력을 INSERT한다.

    Args:
        started_at: 프로시저 시작 wall-clock 시각 (datetime, UTC)
        completed_at: 프로시저 완료 wall-clock 시각 (실패/스킵 시 None)
        duration_sec: time.monotonic() 기반 소요 시간 (초)
    """
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

    spark.sql(f"""
        INSERT INTO {catalog}.ops_bronze.maintenance_watermark
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
    """)


def _escape_sql(s: str) -> str:
    """SQL 문자열 리터럴 내 싱글 쿼트를 이스케이프한다."""
    return s.replace("'", "''") if s else ""


# ---------------------------------------------------------------------------
# Bulk Query for Time-based Execution
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
# Procedure Result Parsing
# ---------------------------------------------------------------------------


def _parse_rewrite_result(result_rows) -> tuple[int, int]:
    """rewrite_data_files / rewrite_position_delete_files 결과에서 파일 수를 추출한다."""
    if result_rows:
        row = result_rows[0]
        return (
            getattr(row, "rewritten_data_files_count", 0) or 0,
            getattr(row, "added_data_files_count", 0) or 0,
        )
    return 0, 0


# ---------------------------------------------------------------------------
# Procedure Runners
# ---------------------------------------------------------------------------


def run_compaction(
    spark: SparkSession,
    catalog: str,
    dag_id: str,
    full_table_name: str,
    batch_id: int | None = None,
) -> None:
    """rewrite_data_files + expire_snapshots를 순차 실행한다.

    rewrite_data_files 실패 시 expire_snapshots는 실행하지 않고 skipped로 기록한다.
    예외를 전파하지 않는다.
    """
    logger = SparkLoggerManager().get_logger()
    _prefix, bronze_schema, table_name = full_table_name.split(".")

    # --- rewrite_data_files ---
    rewrite_ok = False
    wall_start = datetime.now(UTC)
    mono_start = time.monotonic()
    try:
        logger.info(f"rewrite_data_files: {full_table_name}")
        result = spark.sql(f"CALL {catalog}.system.rewrite_data_files(table => '{full_table_name}')").collect()
        duration = time.monotonic() - mono_start
        wall_end = datetime.now(UTC)
        rewritten, added = _parse_rewrite_result(result)
        record_maintenance(
            spark,
            catalog,
            dag_id,
            bronze_schema,
            table_name,
            "rewrite_data_files",
            wall_start,
            wall_end,
            duration,
            "success",
            rewritten_files_count=rewritten,
            added_files_count=added,
            batch_id=batch_id,
        )
        logger.info(f"rewrite_data_files completed: {full_table_name} (rewritten={rewritten}, added={added})")
        rewrite_ok = True
    except Exception as e:
        duration = time.monotonic() - mono_start
        wall_end = datetime.now(UTC)
        error_msg = str(e)[:500]
        record_maintenance(
            spark,
            catalog,
            dag_id,
            bronze_schema,
            table_name,
            "rewrite_data_files",
            wall_start,
            wall_end,
            duration,
            "failed",
            error_message=error_msg,
            batch_id=batch_id,
        )
        logger.warn(f"rewrite_data_files failed: {full_table_name}: {error_msg}")

    # --- expire_snapshots ---
    wall_start = datetime.now(UTC)
    mono_start = time.monotonic()
    if not rewrite_ok:
        record_maintenance(
            spark,
            catalog,
            dag_id,
            bronze_schema,
            table_name,
            "expire_snapshots",
            wall_start,
            None,
            0.0,
            "skipped",
            error_message="Skipped due to preceding procedure failure",
            batch_id=batch_id,
        )
        logger.warn(f"expire_snapshots skipped: {full_table_name} (preceding failure)")
        return

    try:
        logger.info(f"expire_snapshots: {full_table_name}")
        spark.sql(f"CALL {catalog}.system.expire_snapshots(table => '{full_table_name}')")
        duration = time.monotonic() - mono_start
        wall_end = datetime.now(UTC)
        record_maintenance(
            spark,
            catalog,
            dag_id,
            bronze_schema,
            table_name,
            "expire_snapshots",
            wall_start,
            wall_end,
            duration,
            "success",
            batch_id=batch_id,
        )
        logger.info(f"expire_snapshots completed: {full_table_name}")
    except Exception as e:
        duration = time.monotonic() - mono_start
        wall_end = datetime.now(UTC)
        error_msg = str(e)[:500]
        record_maintenance(
            spark,
            catalog,
            dag_id,
            bronze_schema,
            table_name,
            "expire_snapshots",
            wall_start,
            wall_end,
            duration,
            "failed",
            error_message=error_msg,
            batch_id=batch_id,
        )
        logger.warn(f"expire_snapshots failed: {full_table_name}: {error_msg}")


def run_position_delete_compaction(
    spark: SparkSession,
    catalog: str,
    dag_id: str,
    full_table_name: str,
    batch_id: int | None = None,
) -> None:
    """rewrite_position_delete_files만 단독 실행한다. 예외를 전파하지 않는다."""
    logger = SparkLoggerManager().get_logger()
    _prefix, bronze_schema, table_name = full_table_name.split(".")

    wall_start = datetime.now(UTC)
    mono_start = time.monotonic()
    try:
        logger.info(f"rewrite_position_delete_files: {full_table_name}")
        result = spark.sql(
            f"CALL {catalog}.system.rewrite_position_delete_files(table => '{full_table_name}')"
        ).collect()
        duration = time.monotonic() - mono_start
        wall_end = datetime.now(UTC)
        rewritten, added = _parse_rewrite_result(result)
        record_maintenance(
            spark,
            catalog,
            dag_id,
            bronze_schema,
            table_name,
            "rewrite_position_delete_files",
            wall_start,
            wall_end,
            duration,
            "success",
            rewritten_files_count=rewritten,
            added_files_count=added,
            batch_id=batch_id,
        )
        logger.info(
            f"rewrite_position_delete_files completed: {full_table_name} (rewritten={rewritten}, added={added})"
        )
    except Exception as e:
        duration = time.monotonic() - mono_start
        wall_end = datetime.now(UTC)
        error_msg = str(e)[:500]
        record_maintenance(
            spark,
            catalog,
            dag_id,
            bronze_schema,
            table_name,
            "rewrite_position_delete_files",
            wall_start,
            wall_end,
            duration,
            "failed",
            error_message=error_msg,
            batch_id=batch_id,
        )
        logger.warn(f"rewrite_position_delete_files failed: {full_table_name}: {error_msg}")


def run_orphan_cleanup(
    spark: SparkSession,
    catalog: str,
    dag_id: str,
    full_table_name: str,
    orphan_older_than_days: int = 3,
) -> None:
    """remove_orphan_files를 실행한다. 예외를 전파하지 않는다."""
    logger = SparkLoggerManager().get_logger()
    _prefix, bronze_schema, table_name = full_table_name.split(".")

    older_than_ts = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    wall_start = datetime.now(UTC)
    mono_start = time.monotonic()
    try:
        logger.info(f"remove_orphan_files: {full_table_name} (older_than_days={orphan_older_than_days})")
        spark.sql(f"""
            CALL {catalog}.system.remove_orphan_files(
                table => '{full_table_name}',
                older_than => TIMESTAMP '{older_than_ts}' - INTERVAL {orphan_older_than_days} DAYS
            )
        """)
        duration = time.monotonic() - mono_start
        wall_end = datetime.now(UTC)
        record_maintenance(
            spark,
            catalog,
            dag_id,
            bronze_schema,
            table_name,
            "remove_orphan_files",
            wall_start,
            wall_end,
            duration,
            "success",
        )
        logger.info(f"remove_orphan_files completed: {full_table_name}")
    except Exception as e:
        duration = time.monotonic() - mono_start
        wall_end = datetime.now(UTC)
        error_msg = str(e)[:500]
        record_maintenance(
            spark,
            catalog,
            dag_id,
            bronze_schema,
            table_name,
            "remove_orphan_files",
            wall_start,
            wall_end,
            duration,
            "failed",
            error_message=error_msg,
        )
        logger.warn(f"remove_orphan_files failed: {full_table_name}: {error_msg}")


# ---------------------------------------------------------------------------
# Watermark Data Purge
# ---------------------------------------------------------------------------


def purge_old_watermark_records(spark: SparkSession, catalog: str, retention_days: int = 14) -> None:
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

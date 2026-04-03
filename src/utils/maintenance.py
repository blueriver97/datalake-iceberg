"""
Iceberg Maintenance Utilities — 유지보수 프로시저 실행기

- ProcessedTableTracker: 스레드 안전한 수정 테이블 추적기
- 프로시저 실행 함수 (compaction, position delete compaction, orphan cleanup)

Watermark 테이블 관리(생성, 기록, 조회, 정리)는 utils/watermark.py 참조.
"""

import threading
import time
from datetime import UTC, datetime, timedelta

from pyspark.sql import SparkSession

from utils.spark import SparkLoggerManager
from utils.watermark import append_maintenance_watermark

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
        append_maintenance_watermark(
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
        append_maintenance_watermark(
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
        append_maintenance_watermark(
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
        append_maintenance_watermark(
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
        append_maintenance_watermark(
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
        append_maintenance_watermark(
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
        append_maintenance_watermark(
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

    older_than_ts = (datetime.now(UTC) - timedelta(days=orphan_older_than_days)).strftime("%Y-%m-%d %H:%M:+%S")

    wall_start = datetime.now(UTC)
    mono_start = time.monotonic()
    try:
        logger.info(f"remove_orphan_files: {full_table_name} (older_than_days={orphan_older_than_days})")
        spark.sql(f"""
            CALL {catalog}.system.remove_orphan_files(
                table => '{full_table_name}',
                older_than => TIMESTAMP '{older_than_ts}'
            )
        """)
        duration = time.monotonic() - mono_start
        wall_end = datetime.now(UTC)
        append_maintenance_watermark(
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
        append_maintenance_watermark(
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

"""
Iceberg Maintenance — watermark 정리 + compaction + orphan cleanup

실행 순서:
  1. watermark 레코드 정리 (키별 최신 1건 보존, retention 이전 삭제)
  2. watermark 테이블 compaction + expire_snapshots (cdc_watermark, maintenance_watermark)
  3. bronze 스키마 remove_orphan_files

Airflow에서 spark-submit으로 실행:
  spark-submit --py-files utils.zip iceberg_maintenance.py \
    --dag-id "iceberg_maintenance" \
    --schemas "store_bronze" \
    --retention-days 7
"""

from argparse import ArgumentParser

from pyspark.sql import SparkSession

from utils.maintenance import run_compaction, run_orphan_cleanup
from utils.settings import Settings
from utils.spark import SparkLoggerManager, create_spark_session
from utils.watermark import ensure_watermark_tables, purge_watermarks

OPS_WATERMARK_TABLES = [
    "ops_bronze.cdc_watermark",
    "ops_bronze.maintenance_watermark",
]


def discover_tables(spark: SparkSession, catalog: str, schemas: list[str]) -> list[str]:
    """지정된 스키마의 모든 Iceberg 테이블을 조회한다."""
    logger = SparkLoggerManager().get_logger()
    tables: list[str] = []
    for schema in schemas:
        schema = schema.strip().lower()
        full_schema = f"{catalog}.{schema}"
        try:
            rows = spark.sql(f"SHOW TABLES IN {full_schema}").select("tableName").collect()
            tables.extend([f"{full_schema}.{row.tableName}" for row in rows])
        except Exception as e:
            logger.warn(f"Failed to list tables in {full_schema}: {e}")
    logger.info(f"Discovered {len(tables)} tables across {len(schemas)} schema(s)")
    return tables


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--dag-id", type=str, default="manual", help="DAG 식별자 (watermark 기록용, 기본값: manual)")
    parser.add_argument("--retention-days", type=int, default=7, help="watermark 보관 일수 (기본값: 7)")
    parser.add_argument("--schemas", type=str, default="", help="orphan cleanup 대상 bronze 스키마 (쉼표 구분)")
    parser.add_argument("--orphan-older-than-days", type=int, default=3, help="orphan 파일 경과 일수 (기본값: 3)")
    parser.add_argument("--env-file", type=str, default=".env", help="환경 설정 파일 경로 (기본값: .env)")
    args = parser.parse_args()

    settings = Settings(_env_file=args.env_file)
    dag_id = args.dag_id

    spark = create_spark_session("iceberg_maintenance", settings)

    logger_manager = SparkLoggerManager()
    logger_manager.setup(spark)
    logger = logger_manager.get_logger()

    ensure_watermark_tables(spark, settings.CATALOG, settings.WAREHOUSE)

    # -----------------------------------------------------------------------
    # 1단계: watermark 레코드 정리 (키별 최신 1건 보존)
    # -----------------------------------------------------------------------
    logger.info(f"=== Step 1: Purging watermark records (retention={args.retention_days}d) ===")
    purge_watermarks(spark, settings.CATALOG, args.retention_days)

    # -----------------------------------------------------------------------
    # 2단계: watermark 테이블 compaction + expire_snapshots
    # -----------------------------------------------------------------------
    logger.info("=== Step 2: Watermark table compaction ===")
    for key in OPS_WATERMARK_TABLES:
        full_table_name = f"{settings.CATALOG}.{key}"
        if spark.catalog.tableExists(full_table_name):
            run_compaction(spark, settings.CATALOG, dag_id, full_table_name)

    # -----------------------------------------------------------------------
    # 3단계: bronze 스키마 remove_orphan_files
    # -----------------------------------------------------------------------
    if args.schemas:
        schemas = [s.strip() for s in args.schemas.split(",")]
        all_tables = discover_tables(spark, settings.CATALOG, schemas)
        if all_tables:
            logger.info(f"=== Step 3: remove_orphan_files ({len(all_tables)} tables) ===")
            for i, full_table_name in enumerate(all_tables, 1):
                logger.info(f"[{i}/{len(all_tables)}] {full_table_name}")
                run_orphan_cleanup(spark, settings.CATALOG, dag_id, full_table_name, args.orphan_older_than_days)

    logger.info("Watermark maintenance completed.")
    spark.stop()

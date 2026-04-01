"""
Iceberg Heavy Maintenance — remove_orphan_files + ops_bronze compaction + watermark purge.

별도 Airflow DAG에서 하루 1회 실행한다.

실행 순서:
  1. watermark 레코드 정리 (14일 이전 DELETE)
  2. ops_bronze compaction + expire_snapshots (cdc_watermark, maintenance_watermark)
  3. bronze 스키마 remove_orphan_files

Airflow에서 spark-submit으로 실행:
  spark-submit --py-files utils.zip iceberg_maintenance.py \
    --dag-id "maintenance_iceberg" \
    --schemas "store_bronze,ops_bronze"
"""

from argparse import ArgumentParser

from pyspark.sql import SparkSession

from utils.maintenance import (
    ensure_watermark_tables,
    get_last_completed_map,
    purge_old_watermark_records,
    run_compaction,
    run_orphan_cleanup,
    should_run,
)
from utils.settings import Settings
from utils.spark_logging import SparkLoggerManager

# ---------------------------------------------------------------------------
# Table Discovery
# ---------------------------------------------------------------------------

OPS_BRONZE = "ops_bronze"


def discover_tables(spark: SparkSession, catalog: str, schemas: list[str]) -> list[str]:
    """지정된 스키마의 모든 Iceberg 테이블을 조회한다.

    Returns:
        ["{catalog}.{schema}.{table}", ...] 형태의 전체 테이블 목록
    """
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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--dag-id", type=str, required=True, help="DAG 식별자 (watermark 기록용)")
    parser.add_argument("--schemas", type=str, required=True, help="대상 스키마 목록 (쉼표 구분)")
    parser.add_argument(
        "--orphan-older-than-days", type=int, default=3, help="remove_orphan_files 대상 파일 경과 일수 (기본값: 3)"
    )
    parser.add_argument(
        "--watermark-retention-days", type=int, default=14, help="watermark 레코드 보관 일수 (기본값: 14)"
    )
    parser.add_argument(
        "--compaction-interval",
        type=int,
        default=86400,
        help="ops_bronze compaction 실행 간격 초 (기본값: 86400 = 24시간)",
    )
    parser.add_argument(
        "--orphan-interval", type=int, default=86400, help="remove_orphan_files 실행 간격 초 (기본값: 86400 = 24시간)"
    )
    parser.add_argument("--env-file", type=str, default=".env", help="환경 설정 파일 경로 (기본값: .env)")
    args = parser.parse_args()

    settings = Settings(_env_file=args.env_file)
    dag_id = args.dag_id
    schemas = [s.strip() for s in args.schemas.split(",")]

    spark = (
        SparkSession.builder.appName("iceberg_maintenance")
        .config("spark.sql.defaultCatalog", settings.CATALOG)
        .config(f"spark.sql.catalog.{settings.CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{settings.CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{settings.CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{settings.CATALOG}.warehouse", settings.WAREHOUSE)
        .config(f"spark.sql.catalog.{settings.CATALOG}.s3.path-style-access", "true")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
        )
        .config("spark.sql.caseSensitive", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    # 로거 초기화
    logger_manager = SparkLoggerManager()
    logger_manager.setup(spark)
    logger = logger_manager.get_logger()

    # Watermark 테이블 초기화
    ensure_watermark_tables(spark, settings.CATALOG, settings.WAREHOUSE)

    # -----------------------------------------------------------------------
    # 1단계: watermark 레코드 정리 (14일 이전 DELETE)
    # -----------------------------------------------------------------------
    logger.info(f"=== Step 1: Purging watermark records older than {args.watermark_retention_days} days ===")
    purge_old_watermark_records(spark, settings.CATALOG, args.watermark_retention_days)

    # -----------------------------------------------------------------------
    # 2단계: ops_bronze compaction + expire_snapshots
    # -----------------------------------------------------------------------
    ops_tables = discover_tables(spark, settings.CATALOG, [OPS_BRONZE])
    if ops_tables:
        logger.info(f"=== Step 2: ops_bronze compaction ({len(ops_tables)} tables) ===")
        ops_keys = []
        for full_table_name in ops_tables:
            _catalog, bronze_schema, table_name = full_table_name.split(".")
            ops_keys.append(f"{bronze_schema}.{table_name}")

        ops_compaction_map = get_last_completed_map(
            spark,
            settings.CATALOG,
            ops_keys,
            "rewrite_data_files",
        )
        for full_table_name in ops_tables:
            _catalog, bronze_schema, table_name = full_table_name.split(".")
            key = f"{bronze_schema}.{table_name}"
            if should_run(ops_compaction_map.get(key), args.compaction_interval):
                run_compaction(spark, settings.CATALOG, dag_id, full_table_name)

    # -----------------------------------------------------------------------
    # 3단계: bronze 스키마 remove_orphan_files
    # -----------------------------------------------------------------------
    bronze_schemas = [s for s in schemas if s != OPS_BRONZE]
    all_tables = discover_tables(spark, settings.CATALOG, bronze_schemas)
    if all_tables:
        logger.info(f"=== Step 3: remove_orphan_files ({len(all_tables)} tables) ===")
        all_keys = []
        for full_table_name in all_tables:
            _catalog, bronze_schema, table_name = full_table_name.split(".")
            all_keys.append(f"{bronze_schema}.{table_name}")

        orphan_map = get_last_completed_map(
            spark,
            settings.CATALOG,
            all_keys,
            "remove_orphan_files",
        )
        for i, full_table_name in enumerate(all_tables, 1):
            _catalog, bronze_schema, table_name = full_table_name.split(".")
            key = f"{bronze_schema}.{table_name}"
            if should_run(orphan_map.get(key), args.orphan_interval):
                logger.info(f"[{i}/{len(all_tables)}] {full_table_name}")
                run_orphan_cleanup(
                    spark,
                    settings.CATALOG,
                    dag_id,
                    full_table_name,
                    orphan_older_than_days=args.orphan_older_than_days,
                )

    logger.info("Iceberg maintenance completed.")
    spark.stop()

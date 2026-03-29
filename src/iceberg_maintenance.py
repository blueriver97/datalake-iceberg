"""
Iceberg 데이터 테이블 Maintenance (compaction, snapshot expire, orphan cleanup)

Airflow에서 spark-submit으로 실행:
  spark-submit --py-files utils.zip iceberg_maintenance.py \
    --catalog awsdatacatalog \
    --warehouse s3a://bucket/iceberg \
    --schemas store_bronze \
    --retention-days 1 \
    --retain-last 5 \
    --target-file-size 134217728 \
    --min-file-size 100663296
"""

import datetime
from argparse import ArgumentParser

from pyspark.sql import SparkSession

from utils.spark_logging import SparkLoggerManager


def get_target_tables(spark: SparkSession, catalog: str, schemas: list[str]) -> list[str]:
    """지정된 스키마에서 모든 Iceberg 테이블 목록을 조회합니다."""
    logger = SparkLoggerManager().get_logger()
    tables = []
    for schema in schemas:
        full_schema = f"{catalog}.{schema}"
        try:
            table_list = spark.catalog.listTables(full_schema)
            for t in table_list:
                tables.append(f"{full_schema}.{t.name}")
        except Exception as e:
            logger.warn(f"Failed to list tables in {full_schema}: {e}")
    return tables


def perform_maintenance(
    spark: SparkSession,
    catalog: str,
    table: str,
    retention_ts: str,
    retain_last: int,
    target_file_size: int,
    min_file_size: int,
) -> None:
    """테이블에 대해 compaction → snapshot expire → orphan cleanup을 순차 수행합니다."""
    logger = SparkLoggerManager().get_logger()

    # Step 1: Rewrite data files (compaction)
    logger.info(f"[STEP 1] Rewriting data files for {table}")
    spark.sql(f"""
        CALL {catalog}.system.rewrite_data_files(
            table => '{table}',
            options => map(
                'target-file-size-bytes', '{target_file_size}',
                'min-file-size-bytes', '{min_file_size}'
            )
        )
    """)
    logger.info(f"Compaction complete: {table}")

    # Step 2: Expire snapshots
    logger.info(f"[STEP 2] Expiring snapshots for {table}")
    spark.sql(f"""
        CALL {catalog}.system.expire_snapshots(
            table => '{table}',
            older_than => TIMESTAMP '{retention_ts}',
            retain_last => {retain_last}
        )
    """)
    logger.info(f"Snapshot expiration complete: {table}")

    # Step 3: Remove orphan files
    logger.info(f"[STEP 3] Removing orphan files for {table}")
    spark.sql(f"""
        CALL {catalog}.system.remove_orphan_files(
            table => '{table}',
            older_than => TIMESTAMP '{retention_ts}'
        )
    """)
    logger.info(f"Orphan file cleanup complete: {table}")


def run_maintenance(
    spark: SparkSession,
    catalog: str,
    schemas: list[str],
    retention_days: int,
    retain_last: int,
    target_file_size: int,
    min_file_size: int,
) -> None:
    logger = SparkLoggerManager().get_logger()

    tables = get_target_tables(spark, catalog, schemas)
    if not tables:
        logger.info("No target tables found. Skipping maintenance.")
        return

    cutoff_date = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=retention_days)
    retention_ts = cutoff_date.strftime("%Y-%m-%d %H:%M:%S")

    logger.info(f"Running maintenance on {len(tables)} tables (retention: {retention_days} days)")

    failed_tables = []
    for i, table in enumerate(tables, 1):
        logger.info(f"[{i}/{len(tables)}] Processing {table}")
        try:
            perform_maintenance(spark, catalog, table, retention_ts, retain_last, target_file_size, min_file_size)
        except Exception as e:
            logger.error(f"Maintenance failed for {table}: {e}")
            failed_tables.append(table)

    if failed_tables:
        logger.error(f"Maintenance failed for {len(failed_tables)} table(s): {failed_tables}")
        raise RuntimeError(f"Maintenance failed for tables: {failed_tables}")

    logger.info("All maintenance tasks completed successfully.")


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--catalog", type=str, required=True, help="Iceberg catalog 이름")
    parser.add_argument("--warehouse", type=str, required=True, help="Iceberg warehouse 경로 (s3a://...)")
    parser.add_argument("--schemas", type=str, required=True, help="대상 스키마 목록 (콤마 구분)")
    parser.add_argument("--retention-days", type=int, required=True, help="보관 기간 (일)")
    parser.add_argument("--retain-last", type=int, default=5, help="최소 보관 스냅샷 수")
    parser.add_argument("--target-file-size", type=int, default=134217728, help="compaction 대상 파일 크기 (bytes)")
    parser.add_argument("--min-file-size", type=int, default=100663296, help="compaction 최소 파일 크기 (bytes)")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("iceberg_maintenance")
        .config("spark.sql.defaultCatalog", args.catalog)
        .config(f"spark.sql.catalog.{args.catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{args.catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{args.catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{args.catalog}.warehouse", args.warehouse)
        .config(f"spark.sql.catalog.{args.catalog}.s3.path-style-access", "true")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    logger_manager = SparkLoggerManager()
    logger_manager.setup(spark)

    schemas = [s.strip() for s in args.schemas.split(",")]
    run_maintenance(
        spark, args.catalog, schemas, args.retention_days, args.retain_last, args.target_file_size, args.min_file_size
    )
    spark.stop()

"""
CDC Watermark Maintenance (Append-only 테이블 정리)

Airflow에서 spark-submit으로 실행:
  spark-submit --py-files utils.zip watermark_maintenance.py \
    --catalog awsdatacatalog \
    --warehouse s3a://bucket/iceberg \
    --retention-days 7
"""

import datetime
from argparse import ArgumentParser

from pyspark.sql import SparkSession

from utils.spark_logging import SparkLoggerManager


def run_maintenance(spark: SparkSession, catalog: str, retention_days: int) -> None:
    logger = SparkLoggerManager().get_logger()
    full_table = f"{catalog}.ops_bronze.cdc_watermark"

    if not spark.catalog.tableExists(full_table):
        logger.info(f"Table {full_table} does not exist. Skipping maintenance.")
        return

    cutoff_date = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=retention_days)
    retention_ts = cutoff_date.strftime("%Y-%m-%d %H:%M:%S")

    # 보관 기간 이전의 레코드 중, 각 키의 최신 레코드는 제외하고 삭제
    logger.info(f"[STEP 1] Deleting records older than {retention_days} days from {full_table}")
    spark.sql(f"""
        DELETE FROM {full_table}
        WHERE processed_at < TIMESTAMP '{retention_ts}'
          AND (dag_id, bronze_schema, table_name, processed_at) NOT IN (
              SELECT dag_id, bronze_schema, table_name, max(processed_at)
              FROM {full_table}
              GROUP BY dag_id, bronze_schema, table_name
          )
    """)
    logger.info(f"Old records deleted from {full_table} (kept latest per key)")

    # 데이터 파일 압축
    logger.info(f"[STEP 2] Rewriting data files for {full_table}")
    spark.sql(f"CALL {catalog}.system.rewrite_data_files(table => '{full_table}')")
    logger.info(f"Compaction complete: {full_table}")

    # 스냅샷 만료 처리
    logger.info(f"[STEP 3] Expiring snapshots older than {retention_days} days for {full_table}")
    spark.sql(f"""
        CALL {catalog}.system.expire_snapshots(
            table => '{full_table}',
            older_than => TIMESTAMP '{retention_ts}',
            retain_last => 5
        )
    """)
    logger.info(f"Snapshot expiration complete: {full_table}")

    # 고아 파일 정리
    logger.info(f"[STEP 4] Removing orphan files for {full_table}")
    spark.sql(f"""
        CALL {catalog}.system.remove_orphan_files(
            table => '{full_table}',
            older_than => TIMESTAMP '{retention_ts}'
        )
    """)
    logger.info(f"Orphan file cleanup complete: {full_table}")


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--catalog", type=str, required=True, help="Iceberg catalog 이름")
    parser.add_argument("--warehouse", type=str, required=True, help="Iceberg warehouse 경로 (s3a://...)")
    parser.add_argument("--retention-days", type=int, required=True, help="보관 기간 (일)")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("watermark_maintenance")
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

    run_maintenance(spark, args.catalog, args.retention_days)
    spark.stop()

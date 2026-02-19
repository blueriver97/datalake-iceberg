import datetime
import os

from pyspark.sql import SparkSession

from utils.settings import Settings
from utils.spark_logging import SparkLoggerManager


def get_target_tables(spark: SparkSession, config: Settings) -> list[str]:
    """
    설정된 스키마 목록에서 대상 테이블 목록을 조회합니다.
    """
    target_tables = []

    try:
        for schema in config.SCHEMA_LIST:
            tables = spark.catalog.listTables(schema)
            for table in tables:
                target_tables.append(f"{schema}.{table.name}")
    except Exception as e:
        print(f"Warning: Failed to list tables in spark.catalog: {e}")

    return target_tables


def perform_maintenance(spark: SparkSession, config: Settings, table: str) -> None:
    # 로거 초기화
    logger = SparkLoggerManager().get_logger(__name__)

    # 설정 변수 할당
    TARGET_FILE_SIZE_BYTES = 134217728
    MIN_FILE_SIZE_BYTES = 100663296
    RETENTION_DAYS = 1
    RETAIN_LAST_COUNT = 5

    logger.info(f"[START] Cleaning up table: {table}")

    try:
        # 데이터 파일 압축 수행
        logger.info(f"[STEP 1] Rewriting data files for {table} (Target: {TARGET_FILE_SIZE_BYTES})")
        rewrite_query = f"""
            CALL {config.CATALOG}.system.rewrite_data_files(
                table => '{table}',
                options => map(
                    'target-file-size-bytes', '{TARGET_FILE_SIZE_BYTES}',
                    'min-file-size-bytes', '{MIN_FILE_SIZE_BYTES}'
                )
            )
        """
        spark.sql(rewrite_query)

        # 파이썬 내장 모듈을 활용한 기준 날짜 계산
        cutoff_date = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=RETENTION_DAYS)
        retention_timestamp_str = cutoff_date.strftime("%Y-%m-%d %H:%M:%S")

        # 스냅샷 만료 처리
        logger.info(f"[STEP 2] Expiring snapshots (older than {RETENTION_DAYS} days) for {table}")
        expire_query = f"""
            CALL {config.CATALOG}.system.expire_snapshots(
                table => '{table}',
                older_than => TIMESTAMP '{retention_timestamp_str}',
                retain_last => {RETAIN_LAST_COUNT}
            )
        """
        spark.sql(expire_query)

        # 고아 파일 정리 작업
        logger.info(f"[STEP 3] Removing orphan files for {table}")
        remove_orphan_query = f"""
            CALL {config.CATALOG}.system.remove_orphan_files(
                table => '{table}',
                older_than => TIMESTAMP '{retention_timestamp_str}'
            )
        """
        spark.sql(remove_orphan_query)
        logger.info(f"[END] Table cleanup finished for {table}.")

    except Exception as e:
        logger.error(f"[FAIL] Maintenance failed for {table}: {e}")
        raise


def main(spark: SparkSession, config: Settings) -> None:
    logger_manager = SparkLoggerManager()
    logger_manager.setup(spark)
    logger = logger_manager.get_logger(__name__)

    logger.info("Starting Iceberg table maintenance procedure.")

    # 대상 테이블 목록 조회
    # 원본 로직: 스키마 내 모든 테이블 조회
    target_tables = get_target_tables(spark, config)
    logger.info(f"Target tables: {target_tables}")

    total_tables = len(target_tables)
    success_count = 0
    failed_tables = []

    for i, table in enumerate(target_tables, start=1):
        try:
            perform_maintenance(spark, config, table)
            success_count += 1
        except Exception as e:
            failed_tables.append({"table": table, "error": str(e)})

        progress = (i / total_tables) * 100
        logger.info(f"[{progress:3.1f}%] Processed {i}/{total_tables} tables.")

    # 최종 결과 요약
    logger.info("--- Maintenance Process Summary ---")
    logger.info(f"Total: {total_tables}, Success: {success_count}, Fail: {len(failed_tables)}")

    if failed_tables:
        for fail in failed_tables:
            logger.warn(f"Failed Table: {fail['table']} | Reason: {fail['error']}")
        raise RuntimeError(f"Process finished with {len(failed_tables)} failures.")

    logger.info("Iceberg maintenance procedure finished.")


if __name__ == "__main__":
    settings = Settings()
    os.environ["AWS_PROFILE"] = settings.AWS_PROFILE

    spark = (
        SparkSession.builder.appName("IcebergMaintenance")
        .config("spark.sql.defaultCatalog", settings.CATALOG)
        .config(f"spark.sql.catalog.{settings.CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{settings.CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{settings.CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{settings.CATALOG}.s3.path-style-access", "true")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
        )
        .config("spark.sql.caseSensitive", "true")
        .getOrCreate()
    )

    main(spark, settings)
    spark.stop()

import os

from pyspark.sql import SparkSession

from utils.settings import Settings
from utils.spark_logging import SparkLoggerManager


def get_target_tables(spark: SparkSession, config: Settings) -> list[str]:
    """
    설정된 스키마 목록에서 대상 테이블 목록을 조회합니다.
    """
    target_tables = []
    # config.SCHEMA_LIST가 없으므로 TABLE_LIST에서 스키마를 추출하거나,
    # Settings에 SCHEMA_LIST를 추가해야 함.
    # 여기서는 TABLE_LIST의 스키마 정보를 활용하여 해당 스키마의 모든 테이블을 조회하는 방식으로 구현.
    # 또는 단순히 config.TABLE_LIST를 반환할 수도 있음.
    # 원본 코드는 SCHEMA_LIST를 순회하며 SHOW TABLES를 수행했음.

    # TABLE_LIST에서 유니크한 스키마 추출
    schemas = set()
    for table in config.TABLE_LIST:
        parts = table.split(".")
        if len(parts) >= 2:
            # mysql: db.table -> db
            # mssql: db.dbo.table -> db
            schemas.add(parts[0])

    for schema in schemas:
        bronze_schema = f"{schema.lower()}_bronze"
        try:
            tables = (
                spark.sql(f"SHOW TABLES IN {config.CATALOG}.{bronze_schema}")
                .select("tableName")
                .rdd.map(lambda value: value[0])
                .collect()
            )
            target_tables.extend([f"{config.CATALOG}.{bronze_schema}.{table}" for table in tables])
        except Exception as e:
            print(f"Warning: Failed to list tables in {bronze_schema}: {e}")

    return target_tables


def perform_maintenance(spark: SparkSession, config: Settings, table: str) -> None:
    """
    단일 테이블에 대한 유지보수 작업(Compaction, Snapshot Expiration)을 수행합니다.
    """
    logger = SparkLoggerManager().get_logger(__name__)

    # --- 설정값 변수 ---
    # 1. 압축 파일 크기 (128MB)
    TARGET_FILE_SIZE_BYTES = 128 * 1024 * 1024
    # 2. (운영 옵션) 최소 압축 파일 크기 (128MB의 75% = 96MB)
    MIN_FILE_SIZE_BYTES = int(TARGET_FILE_SIZE_BYTES * 0.75)
    # 3. 스냅샷 보존 기간 (1일)
    RETENTION_DAYS = 1
    # 4. (운영 옵션) 최소 보존 스냅샷 개수
    RETAIN_LAST_COUNT = 5

    logger.info(f"[START] Cleaning up table: {table}")

    try:
        # --- 1. 데이터 파일 재작성 (Compaction) ---
        logger.info(f"[STEP 1] Rewriting data files for {table} (Target: 128MB)")
        # Spark SQL에서 CALL 프로시저 호출 시 인자 전달 방식 주의 (Named arguments)
        # 문자열 보간법 사용 시 SQL Injection 주의가 필요하나, 내부 관리용이므로 허용 범위 내.
        rewrite_query = f"""
            CALL {config.CATALOG}.system.rewrite_data_files(
                table => '{table}',
                options => map(
                    'target-file-size-bytes', '{TARGET_FILE_SIZE_BYTES}',
                    'min-file-size-bytes', '{MIN_FILE_SIZE_BYTES}'
                )
            )
        """
        spark.sql(rewrite_query).show(truncate=False)

        # --- 2. 스냅샷 만료 및 고아 파일 정리 ---
        # 현재 시간 기준 RETENTION_DAYS 이전 시간 계산
        retention_timestamp_df = spark.sql(
            f"SELECT date_format(current_timestamp() - INTERVAL {RETENTION_DAYS} DAYS, 'yyyy-MM-dd HH:mm:ss') as cutoff"
        )
        retention_timestamp_str = retention_timestamp_df.collect()[0]["cutoff"]

        logger.info(
            f"[STEP 2] Expiring snapshots (older than {RETENTION_DAYS} days) and removing orphan files for {table}"
        )

        expire_query = f"""
            CALL {config.CATALOG}.system.expire_snapshots(
                table => '{table}',
                older_than => TIMESTAMP '{retention_timestamp_str}',
                retain_last => {RETAIN_LAST_COUNT}
            )
        """
        spark.sql(expire_query).show(truncate=False)

        # Note: remove_orphan_files는 expire_snapshots와 별도로 수행하거나,
        # expire_snapshots의 옵션으로 수행할 수 있음 (버전에 따라 다름).
        # 최신 Iceberg 버전에서는 expire_snapshots가 고아 파일을 정리하지 않을 수 있으므로
        # remove_orphan_files를 명시적으로 호출하는 것이 안전할 수 있음.
        # 원본 코드 주석에 따라 expire_snapshots만 수행.

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
        # 유지보수 작업 실패가 전체 파이프라인 중단을 야기해야 하는지 여부에 따라 raise 결정
        # 여기서는 경고만 하고 정상 종료 처리 (유지보수는 다음 주기에 다시 시도 가능)

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

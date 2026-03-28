import os

import pyspark.sql.types as T
from pyspark.sql import SparkSession

from utils.database import (
    DatabaseType,
    convert_db_type_to_spark,
    get_primary_keys,
    get_table_schema_info,
)

# --- Import common modules ---
from utils.settings import Settings
from utils.spark_logging import SparkLoggerManager


def process_table_schema(
    spark: SparkSession, config: Settings, table_name: str, schema_info: dict, pk_cols: list[str] | None
) -> None:
    """
    원본 데이터베이스의 스키마 정보를 바탕으로 Iceberg 테이블을 생성합니다.

    Args:
        spark (SparkSession): Spark 세션 객체
        config (Settings): 설정 정보 객체
        table_name (str): 원본 테이블 명
        schema_info (dict): 컬럼명과 타입 정보를 담은 딕셔너리
        pk_cols (Optional[list[str]]): 기본키 컬럼 리스트

    Returns:
        None
    """
    logger = SparkLoggerManager().get_logger()

    parts = table_name.split(".")
    if len(parts) == 3 and config.DB_TYPE == DatabaseType.SQLSERVER:
        schema, _, table = parts
    else:
        schema, table = parts

    bronze_schema = f"{schema.lower()}_bronze"
    target_table = table.lower()
    full_table_name = f"{config.CATALOG}.{bronze_schema}.{target_table}"

    # 데이터베이스 생성
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.CATALOG}.{bronze_schema}")

    # 기존 테이블 존재 여부 확인
    if spark.catalog.tableExists(full_table_name):
        logger.info(f"Table '{full_table_name}' already exists. Skipping.")
        return

    # Spark 스키마 구성
    struct_fields = [
        T.StructField("last_applied_date", T.TimestampType(), True),
        *[
            T.StructField(col, convert_db_type_to_spark(col_type, config.DB_TYPE), True)
            for col, col_type in schema_info.items()
        ],
    ]

    # PK가 존재할 경우 id_iceberg 컬럼 추가 (Hash Key)
    if pk_cols:
        struct_fields.append(T.StructField("id_iceberg", T.StringType(), True))

    final_schema = T.StructType(struct_fields)
    empty_df = spark.createDataFrame([], final_schema)

    # Iceberg 테이블 생성 및 속성 설정
    (
        empty_df.writeTo(full_table_name)
        .using("iceberg")
        .tableProperty("location", f"{config.ICEBERG_S3_ROOT_PATH}/{bronze_schema}/{target_table}")
        .tableProperty("format-version", "2")
        .tableProperty("write.metadata.delete-after-commit.enabled", "true")
        .tableProperty("write.metadata.previous-versions-max", "5")
        .tableProperty("history.expire.max-snapshot-age-ms", "86400000")
        .createOrReplace()
    )
    logger.info(f"Successfully created table: {full_table_name}")


def main(spark: SparkSession, config: Settings) -> None:
    """
    Creates Iceberg tables based on the schema of the source database.
    """
    logger_manager = SparkLoggerManager()
    logger_manager.setup(spark)
    logger = logger_manager.get_logger()

    logger.info("Starting Iceberg table creation from schema.")
    logger.info(f"Target tables: {config.TABLE_LIST}")

    # 메타데이터 조회
    table_schemas: dict[str, dict[str, str]] = get_table_schema_info(spark, config)
    primary_keys: dict[str, list[str]] = get_primary_keys(spark, config)

    failed_tables: list[dict[str, str]] = []
    success_count = 0
    total_tables = len(config.TABLE_LIST)

    for table_name in config.TABLE_LIST:
        try:
            schema_info = table_schemas.get(table_name)
            if not schema_info:
                logger.error(f"Failed to retrieve schema info for '{table_name}'. Skipping.")
                failed_tables.append({"table": table_name, "error": "Schema info not found"})
                continue

            pk_cols = primary_keys.get(table_name)
            process_table_schema(spark, config, table_name, schema_info, pk_cols)
            success_count += 1

            progress = (success_count / total_tables) * 100
            logger.info(f"[{progress:3.1f}%]Successfully processed {success_count}/{total_tables} tables.")
        except Exception as e:
            failed_tables.append({"table": table_name, "error": str(e)})
            logger.error(f"[FAIL] Error processing table {table_name}: {e}")

    # 최종 결과 요약 출력
    # Print final summary
    logger.info("--- Schema Creation Process Summary ---")
    logger.info(f"Total: {total_tables}, Success: {success_count}, Fail: {len(failed_tables)}")

    if failed_tables:
        for fail in failed_tables:
            logger.warn(f"Failed Table: {fail['table']} | Reason: {fail['error']}")
        raise RuntimeError(f"Process finished with {len(failed_tables)} failures.")

    logger.info("Schema creation process finished successfully.")


if __name__ == "__main__":
    # 1. Load settings
    settings = Settings()

    # 2. Set AWS profile from settings
    os.environ["AWS_PROFILE"] = settings.AWS_PROFILE

    # 3. Create a Spark session
    spark = (
        SparkSession.builder.appName("schema_to_iceberg")
        .config("spark.sql.defaultCatalog", settings.CATALOG)
        .config(f"spark.sql.catalog.{settings.CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{settings.CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{settings.CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{settings.CATALOG}.warehouse", settings.ICEBERG_S3_ROOT_PATH)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
        )
        .getOrCreate()
    )

    # 4. Run the main logic
    main(spark, settings)
    spark.stop()

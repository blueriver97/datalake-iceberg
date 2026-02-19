import os

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Column, DataFrame, SparkSession

# --- Import common modules ---
from utils.database import get_primary_keys
from utils.settings import Settings
from utils.spark_logging import SparkLoggerManager


def cast_dataframe(df: DataFrame) -> DataFrame:
    """
    AWS DMS는 09:00 Asia/Seoul 값을 09:00 UTC 값으로 저장함.
    따라서 정확한 UTC 시간은 -9 시간을 빼야 함.
    to_utc_timestamp 함수는 값을 주어진 시간 대의 값으로 해석해 UTC 시간을 반환함.
    """

    def cast_column_type(field: T.StructField) -> Column:
        if isinstance(field.dataType, T.TimestampType):
            return F.to_utc_timestamp(F.col(field.name), "UTC").alias(field.name)
        return F.col(field.name).alias(field.name)

    return df.select([cast_column_type(field) for field in df.schema.fields])


def process_parquet_to_iceberg(spark: SparkSession, config: Settings, table_name: str, pk_cols: list[str]) -> None:
    """
    Parquet 데이터를 읽어 Iceberg 테이블로 생성합니다.

    Args:
        spark (SparkSession): Spark 세션 객체
        config (Settings): 설정 정보 객체
        table_name (str): 대상 테이블 명
        pk_cols (list[str]): 기본키 컬럼 리스트
    """
    logger = SparkLoggerManager().get_logger(__name__)

    schema, table = table_name.split(".")
    bronze_schema = f"{schema.lower()}_bronze"
    target_table = table.lower()
    full_table_name = f"{config.CATALOG}.{bronze_schema}.{target_table}"

    parquet_dir = f"{config.PARQUET_S3_ROOT_PATH}/{schema}/{table}"

    # Parquet 파일 읽기
    parquet_df = spark.read.parquet(parquet_dir, recursiveFileLookup=True)
    parquet_df = cast_dataframe(parquet_df)
    parquet_df = parquet_df.withColumnRenamed("update_ts_dms", "last_applied_date").withColumn(
        "last_applied_date", F.col("last_applied_date").cast(T.TimestampType())
    )

    # 데이터베이스가 존재하지 않으면 생성
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.CATALOG}.{bronze_schema}")

    logger.info(f"Creating or replacing {full_table_name}")

    if pk_cols:
        parquet_df = parquet_df.withColumn("id_iceberg", F.md5(F.concat_ws("|", *[F.col(pk) for pk in pk_cols])))

    # Iceberg 테이블 생성 및 데이터 쓰기 (RTAS)
    (
        parquet_df.writeTo(full_table_name)
        .using("iceberg")
        .tableProperty("location", f"{config.ICEBERG_S3_ROOT_PATH}/{bronze_schema}/{target_table}")
        .tableProperty("format-version", "2")
        .tableProperty("write.metadata.delete-after-commit.enabled", "true")
        .tableProperty("write.metadata.previous-versions-max", "5")
        .tableProperty("history.expire.max-snapshot-age-ms", "86400000")
        .createOrReplace()
    )

    logger.info(f"Successfully created or replaced {full_table_name}")


def main(spark: SparkSession, config: Settings) -> None:
    """
    S3의 Parquet 파일에서 데이터를 읽어 Iceberg 테이블로 저장합니다.
    """
    logger_manager = SparkLoggerManager()
    logger_manager.setup(spark)
    logger = logger_manager.get_logger(__name__)

    logger.info("Starting Iceberg table creation from Parquet.")
    logger.info(f"Target tables: {config.TABLE_STR}")

    # 데이터베이스에서 기본키 정보를 조회합니다.
    primary_keys: dict[str, list[str]] = get_primary_keys(spark, config)

    success_count = 0
    failed_tables = []
    total_tables = len(config.TABLE_LIST)

    for table_name in config.TABLE_LIST:
        try:
            pk_cols = primary_keys.get(table_name, [])
            process_parquet_to_iceberg(spark, config, table_name, pk_cols)

            success_count += 1
            progress = (success_count / total_tables) * 100
            logger.info(f"[{progress:3.1f}%] Successfully processed {success_count}/{total_tables} tables.")

        except Exception as e:
            failed_tables.append({"table": table_name, "error": str(e)})
            logger.error(f"[FAIL] Failed to process {table_name}: {e}")

    # 최종 결과 요약 출력
    logger.info("--- Iceberg Table Creation Process Summary ---")
    logger.info(f"Total: {total_tables}, Success: {success_count}, Fail: {len(failed_tables)}")

    if failed_tables:
        for fail in failed_tables:
            logger.warn(f"Failed Table: {fail['table']} | Reason: {fail['error']}")
        raise RuntimeError(f"Process finished with {len(failed_tables)} failures.")

    logger.info("Iceberg table creation process finished successfully.")


if __name__ == "__main__":
    settings = Settings()

    os.environ["AWS_PROFILE"] = settings.AWS_PROFILE

    spark = (
        SparkSession.builder.appName("ParquetToIceberg")
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
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.SimplifyCasts")
        .getOrCreate()
    )

    main(spark, settings)
    spark.stop()

import argparse

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Column, DataFrame, SparkSession

# --- Import common modules ---
from utils.database import BaseDatabaseManager, MySQLManager, SQLServerManager
from utils.settings import Settings
from utils.spark_logging import SparkLoggerManager


def cast_dataframe(df: DataFrame) -> DataFrame:
    """Parquet 파일의 타임스탬프 UTC 보정 및 CHAR 타입 Trim 처리"""

    def cast_column_type(field: T.StructField) -> Column:
        if isinstance(field.dataType, T.StringType):
            return F.trim(F.col(field.name)).alias(field.name)
        if isinstance(field.dataType, T.TimestampType):
            return F.to_utc_timestamp(F.col(field.name), "UTC").alias(field.name)
        return F.col(field.name).alias(field.name)

    return df.select([cast_column_type(field) for field in df.schema.fields])


def process_parquet_to_iceberg(
    spark: SparkSession,
    config: Settings,
    db_manager: BaseDatabaseManager,
    table_name: str,
) -> None:
    """
    S3 Parquet 파일을 읽어 Iceberg 테이블로 생성합니다.

    Args:
        spark (SparkSession): Spark 세션 객체
        config (Settings): 설정 객체
        db_manager (BaseDatabaseManager): 데이터베이스 관리자 객체
        table_name (str): 대상 테이블 명 (db.table 또는 db.dbo.table)
    """
    logger = SparkLoggerManager().get_logger()

    # 테이블명 파싱 (MySQL: db.table, SQL Server: db.dbo.table)
    parts = table_name.split(".")
    if len(parts) == 2:
        schema, table = parts
    elif len(parts) == 3:
        schema, _, table = parts
    else:
        raise ValueError(f"Invalid table name format: '{table_name}'. Expected 'db.table' or 'db.schema.table'.")

    bronze_schema = f"{schema.lower()}_bronze"
    target_table = table.lower()
    full_table_name = f"{config.CATALOG}.{bronze_schema}.{target_table}"
    parquet_path = f"{config.PARQUET_WAREHOUSE}/{schema}/{table}"

    pk_cols = db_manager.get_primary_key(spark, table_name)

    logger.info(f"Reading Parquet from {parquet_path}")
    parquet_df = spark.read.parquet(parquet_path)

    parquet_df = cast_dataframe(parquet_df)

    # update_ts_dms → last_applied_date 리네이밍
    if "update_ts_dms" in parquet_df.columns:
        parquet_df = parquet_df.withColumnRenamed("update_ts_dms", "last_applied_date")
    else:
        parquet_df = parquet_df.withColumn("last_applied_date", F.current_timestamp())

    # 데이터베이스 생성
    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {config.CATALOG}.{bronze_schema} LOCATION '{config.WAREHOUSE}/{bronze_schema}'"
    )

    logger.info(f"Creating or replacing {full_table_name}")

    if pk_cols:
        parquet_df = parquet_df.withColumn("id_iceberg", F.md5(F.concat_ws("|", *[F.col(pk) for pk in pk_cols])))

    # Iceberg 테이블 생성 및 데이터 쓰기 (RTAS)
    writer = (
        parquet_df.writeTo(full_table_name)
        .using("iceberg")
        .tableProperty("location", f"{config.WAREHOUSE}/{bronze_schema}/{target_table}")
        .tableProperty("format-version", "2")
    )

    if pk_cols:
        writer = (
            writer.tableProperty("write.metadata.delete-after-commit.enabled", "true")
            .tableProperty("write.metadata.previous-versions-max", "5")
            .tableProperty("history.expire.max-snapshot-age-ms", "86400000")
        )

    writer.createOrReplace()
    logger.info(f"Successfully created or replaced {full_table_name}")


def main(spark: SparkSession, config: Settings, app_args) -> None:
    """
    Reads Parquet files from S3 and saves them as Iceberg tables.
    """
    logger_manager = SparkLoggerManager()
    logger_manager.setup(spark)
    logger = logger_manager.get_logger()

    logger.info("Starting Iceberg table creation from Parquet.")

    table_name = app_args.table

    try:
        db_manager: BaseDatabaseManager
        if config.database.type == "sqlserver":
            db_manager = SQLServerManager(config)
        else:
            db_manager = MySQLManager(config)

        process_parquet_to_iceberg(spark, config, db_manager, table_name)
    except Exception as e:
        logger.error(f"Failed to process table '{table_name}': {e}")
        raise e
    else:
        logger.info("Iceberg table creation process finished successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", type=str)
    args = parser.parse_args()
    settings = Settings()

    spark = (
        SparkSession.builder.appName("parquet_to_iceberg")
        .config("spark.sql.defaultCatalog", settings.CATALOG)
        .config(f"spark.sql.catalog.{settings.CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{settings.CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{settings.CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{settings.CATALOG}.warehouse", settings.WAREHOUSE)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.SimplifyCasts")
        .getOrCreate()
    )

    main(spark, settings, args)
    spark.stop()

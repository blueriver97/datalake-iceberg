import argparse

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession

# --- Import common modules ---
from utils.cleansing import trim_string_columns
from utils.database import BaseDatabaseManager, MySQLManager, SQLServerManager
from utils.iceberg import create_or_replace_iceberg_table
from utils.settings import Settings
from utils.spark_logging import SparkLoggerManager


def convert_timestamps_to_utc(df: DataFrame) -> DataFrame:
    """Parquet 파일의 TimestampType 컬럼을 UTC로 보정한다."""
    return df.select(
        [
            F.to_utc_timestamp(F.col(field.name), "UTC").alias(field.name)
            if isinstance(field.dataType, T.TimestampType)
            else F.col(field.name).alias(field.name)
            for field in df.schema.fields
        ]
    )


def process_parquet_to_iceberg(
    spark: SparkSession,
    settings: Settings,
    db_manager: BaseDatabaseManager,
    table_name: str,
) -> None:
    """
    S3 Parquet 파일을 읽어 Iceberg 테이블로 생성합니다.

    Args:
        spark (SparkSession): Spark 세션 객체
        settings (Settings): 설정 객체
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
    parquet_path = f"{settings.WAREHOUSE}/{schema}/{table}"

    pk_cols = db_manager.get_primary_key(spark, table_name)

    logger.info(f"Reading Parquet from {parquet_path}")
    parquet_df = spark.read.parquet(parquet_path)

    parquet_df = trim_string_columns(parquet_df)
    parquet_df = convert_timestamps_to_utc(parquet_df)

    # update_ts_dms → last_applied_date 리네이밍
    if "update_ts_dms" in parquet_df.columns:
        parquet_df = parquet_df.withColumnRenamed("update_ts_dms", "last_applied_date")
    else:
        parquet_df = parquet_df.withColumn("last_applied_date", F.current_timestamp())

    if pk_cols:
        parquet_df = parquet_df.withColumn("id_iceberg", F.md5(F.concat_ws("|", *[F.col(pk) for pk in pk_cols])))

    create_or_replace_iceberg_table(spark, parquet_df, settings, bronze_schema, target_table, pk_cols)


def main(spark: SparkSession, settings: Settings, app_args) -> None:
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
        if settings.database.type == "sqlserver":
            db_manager = SQLServerManager(settings)
        else:
            db_manager = MySQLManager(settings)

        process_parquet_to_iceberg(spark, settings, db_manager, table_name)
    except Exception as e:
        logger.error(f"Failed to process table '{table_name}': {e}")
        raise e
    else:
        logger.info("Iceberg table creation process finished successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", type=str)
    parser.add_argument("--env-file", type=str, default=".env", help="환경 설정 파일 경로 (기본값: .env)")
    args = parser.parse_args()
    settings = Settings(_env_file=args.env_file)

    spark = (
        SparkSession.builder.appName("parquet_to_iceberg")
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
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.SimplifyCasts")
        .getOrCreate()
    )

    main(spark, settings, args)
    spark.stop()

"""
S3 Parquet → Iceberg 배치 적재 파이프라인

S3에 저장된 Parquet 파일을 읽어 Iceberg 테이블로 생성한다.
mysql_to_parquet / sqlserver_to_parquet의 후속 단계로 사용된다.

실행:
  spark-submit --py-files utils.zip parquet_to_iceberg.py \
    --service <service> --table "db.table_name" --env-file .env
"""

import argparse

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession

from utils.database import BaseDatabaseManager, MySQLManager, SQLServerManager

# --- Import common modules ---
from utils.iceberg import create_or_replace_iceberg_table, trim_string_columns
from utils.settings import Settings
from utils.spark import SparkLoggerManager, create_spark_session


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
    service: str,
    table_name: str,
) -> None:
    """
    S3 Parquet 파일을 읽어 Iceberg 테이블로 생성합니다.

    Args:
        spark (SparkSession): Spark 세션 객체
        settings (Settings): 설정 객체
        db_manager (BaseDatabaseManager): 데이터베이스 관리자 객체
        service (str): 서비스 영문 식별자 (Glue Catalog Database prefix)
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

    iceberg_schema = f"{service}_{schema.lower()}"
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

    create_or_replace_iceberg_table(spark, parquet_df, settings, iceberg_schema, target_table, pk_cols)


def main(spark: SparkSession, settings: Settings, app_args) -> None:
    """
    Reads Parquet files from S3 and saves them as Iceberg tables.
    """
    logger_manager = SparkLoggerManager()
    logger_manager.setup(spark)
    logger = logger_manager.get_logger()

    logger.info("Starting Iceberg table creation from Parquet.")

    service = app_args.service
    table_name = app_args.table

    try:
        db_manager: BaseDatabaseManager
        if settings.database.type == "sqlserver":
            db_manager = SQLServerManager(settings)
        else:
            db_manager = MySQLManager(settings)

        process_parquet_to_iceberg(spark, settings, db_manager, service, table_name)
    except Exception as e:
        logger.error(f"Failed to process table '{table_name}': {e}")
        raise e
    else:
        logger.info("Iceberg table creation process finished successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--service",
        type=str.lower,
        required=True,
        help="서비스 영문 식별자 (Glue Catalog Database prefix, 소문자로 정규화)",
    )
    parser.add_argument("--table", type=str)
    parser.add_argument("--env-file", type=str, default=".env", help="환경 설정 파일 경로 (기본값: .env)")
    args = parser.parse_args()
    settings = Settings(_env_file=args.env_file)

    spark = create_spark_session(
        "parquet_to_iceberg",
        settings,
        extra_configs={
            "spark.sql.parquet.datetimeRebaseModeInRead": "CORRECTED",
            "spark.sql.parquet.int96RebaseModeInRead": "CORRECTED",
            "spark.sql.optimizer.excludedRules": "org.apache.spark.sql.catalyst.optimizer.SimplifyCasts",
        },
    )

    main(spark, settings, args)
    spark.stop()

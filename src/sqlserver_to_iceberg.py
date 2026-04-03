"""
SQL Server → Iceberg 배치 적재 파이프라인

JDBC로 SQL Server 테이블을 읽어 Iceberg 테이블로 전체 교체(RTAS)한다.

실행:
  spark-submit --py-files utils.zip sqlserver_to_iceberg.py \
    --table "db.dbo.table_name" --num_partition 8 --env-file .env
"""

import argparse

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# --- Import common modules ---
from utils.cleansing import trim_string_columns
from utils.database import BaseDatabaseManager, SQLServerManager
from utils.iceberg import create_or_replace_iceberg_table
from utils.jdbc_reader import read_jdbc_table
from utils.settings import Settings
from utils.spark_logging import SparkLoggerManager


def process_sqlserver_to_iceberg(
    spark: SparkSession,
    settings: Settings,
    db_manager: BaseDatabaseManager,
    table_name: str,
    num_partition: int,
) -> None:
    """
    SQL Server 테이블 데이터를 읽어 Iceberg 테이블로 생성합니다.

    Args:
        spark (SparkSession): Spark 세션 객체
        settings (Settings): 설정 객체
        db_manager (BaseDatabaseManager): 데이터베이스 관리자 객체
        table_name (str): 대상 테이블 명 (db.table)
        num_partition (int): 파티션 개수
    """
    # SQL Server table name format (db.table) parsing
    parts = table_name.split(".")
    if len(parts) == 3:
        schema, _, table = parts
    else:
        raise ValueError(f"Invalid table name format: '{table_name}'. Expected 'db.schema.table'.")

    bronze_schema = f"{schema.lower()}_bronze"
    target_table = table.lower()

    pk_cols = db_manager.get_primary_key(spark, table_name)
    jdbc_df = read_jdbc_table(spark, db_manager, table_name, num_partition, database=schema)

    jdbc_df = trim_string_columns(jdbc_df)
    jdbc_df = jdbc_df.withColumn("last_applied_date", F.current_timestamp())

    if pk_cols:
        jdbc_df = jdbc_df.withColumn("id_iceberg", F.md5(F.concat_ws("|", *[F.col(pk) for pk in pk_cols])))

    create_or_replace_iceberg_table(spark, jdbc_df, settings, bronze_schema, target_table, pk_cols)


def main(spark: SparkSession, settings: Settings, app_args) -> None:
    """
    Reads data from a SQL Server database and saves it as Iceberg tables.
    """
    logger_manager = SparkLoggerManager()
    logger_manager.setup(spark)
    logger = logger_manager.get_logger()

    logger.info("Starting Iceberg table creation from SQL Server.")

    table_name = app_args.table
    num_partition = app_args.num_partition

    try:
        db_manager = SQLServerManager(settings)
        process_sqlserver_to_iceberg(spark, settings, db_manager, table_name, num_partition)
    except Exception as e:
        logger.error(f"Failed to process table '{table_name}': {e}")
        raise e
    else:
        logger.info("Iceberg table creation process finished successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", type=str)
    parser.add_argument("--num_partition", type=int)
    parser.add_argument("--env-file", type=str, default=".env", help="환경 설정 파일 경로 (기본값: .env)")
    args = parser.parse_args()
    settings = Settings(_env_file=args.env_file)

    spark = (
        SparkSession.builder.appName("sqlserver_to_iceberg")
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
        .config("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.SimplifyCasts")
        .getOrCreate()
    )

    main(spark, settings, args)
    spark.stop()

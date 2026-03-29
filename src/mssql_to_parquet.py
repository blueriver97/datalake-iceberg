import argparse

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Column, DataFrame, SparkSession

# --- Import common modules ---
from utils.database import BaseDatabaseManager, SQLServerManager
from utils.settings import Settings
from utils.spark_logging import SparkLoggerManager


def cast_dataframe(df: DataFrame) -> DataFrame:
    """SQL Server CHAR 타입 Trim 처리"""

    def cast_column_type(field: T.StructField) -> Column:
        if isinstance(field.dataType, T.StringType):
            return F.trim(F.col(field.name)).alias(field.name)
        return F.col(field.name).alias(field.name)

    return df.select([cast_column_type(field) for field in df.schema.fields])


def process_mssql_to_parquet(
    spark: SparkSession,
    config: Settings,
    db_manager: BaseDatabaseManager,
    table_name: str,
    num_partition: int,
) -> None:
    """
    SQL Server 테이블 데이터를 읽어 S3에 Parquet 파일로 저장합니다.

    Args:
        spark (SparkSession): Spark 세션 객체
        config (Settings): 설정 객체
        db_manager (BaseDatabaseManager): 데이터베이스 관리자 객체
        table_name (str): 대상 테이블 명 (db.dbo.table)
        num_partition (int): 파티션 개수
    """
    logger = SparkLoggerManager().get_logger()

    # SQL Server table name format (db.dbo.table) parsing
    parts = table_name.split(".")
    if len(parts) == 3:
        schema, _, table = parts
    else:
        raise ValueError(f"Invalid table name format: '{table_name}'. Expected 'db.schema.table'.")

    output_path = f"{config.PARQUET_WAREHOUSE}/{schema}/{table}"

    partition_column = db_manager.get_partition_key(spark, table_name)
    jdbc_options = db_manager.get_jdbc_options(database=schema)
    jdbc_df: DataFrame

    if partition_column:
        logger.info(f"Reading '{table_name}' with partitioning on column '{partition_column}'.")
        bound_query = f"SELECT min({partition_column}) as 'lower', max({partition_column}) as 'upper' FROM {table_name}"
        bound_df = spark.read.format("jdbc").options(**jdbc_options).option("query", bound_query).load()
        bounds = bound_df.first()

        if not bounds or bounds["lower"] is None:
            logger.warn(
                f"Partition column '{partition_column}' has no data for table '{table_name}'. Reading without partitioning."
            )
            jdbc_df = spark.read.format("jdbc").options(**jdbc_options).option("dbtable", table_name).load()
        else:
            jdbc_df = (
                spark.read.format("jdbc")
                .options(**jdbc_options)
                .option("dbtable", table_name)
                .option("partitionColumn", partition_column)
                .option("lowerBound", bounds["lower"])
                .option("upperBound", bounds["upper"])
                .option("numPartitions", num_partition)
                .load()
            )
    else:
        logger.info(f"Reading '{table_name}' without partitioning.")
        jdbc_df = spark.read.format("jdbc").options(**jdbc_options).option("dbtable", table_name).load()

    jdbc_df = cast_dataframe(jdbc_df)
    jdbc_df = jdbc_df.withColumn("update_ts_dms", F.current_timestamp())

    logger.info(f"Writing Parquet to {output_path}")
    jdbc_df.write.mode("overwrite").parquet(output_path)
    logger.info(f"Successfully wrote {table_name} to {output_path}")


def main(spark: SparkSession, config: Settings, app_args) -> None:
    """
    Reads data from a SQL Server database and saves it as Parquet files on S3.
    """
    logger_manager = SparkLoggerManager()
    logger_manager.setup(spark)
    logger = logger_manager.get_logger()

    logger.info("Starting Parquet export from SQL Server.")

    table_name = app_args.table
    num_partition = app_args.num_partition

    try:
        db_manager = SQLServerManager(config)
        process_mssql_to_parquet(spark, config, db_manager, table_name, num_partition)
    except Exception as e:
        logger.error(f"Failed to process table '{table_name}': {e}")
        raise e
    else:
        logger.info("Parquet export process finished successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", type=str)
    parser.add_argument("--num_partition", type=int)
    args = parser.parse_args()
    settings = Settings()

    spark = (
        SparkSession.builder.appName("mssql_to_parquet")
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
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.SimplifyCasts")
        .getOrCreate()
    )

    main(spark, settings, args)
    spark.stop()

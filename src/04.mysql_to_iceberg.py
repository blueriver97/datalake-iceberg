import os

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Column, DataFrame, SparkSession

# --- Import common modules ---
from utils.database import get_jdbc_options, get_partition_key_info, get_primary_keys
from utils.settings import Settings
from utils.spark_logging import SparkLoggerManager


def cast_dataframe(df: DataFrame) -> DataFrame:
    """MySQL CHAR 타입 Trim 처리코드 추가"""

    def cast_column_type(field: T.StructField) -> Column:
        if isinstance(field.dataType, T.StringType):
            return F.trim(F.col(field.name)).alias(field.name)
        return F.col(field.name).alias(field.name)

    return df.select([cast_column_type(field) for field in df.schema.fields])


def process_mysql_to_iceberg(
    spark: SparkSession,
    config: Settings,
    table_name: str,
    jdbc_options: dict,
    primary_keys: dict,
    partition_keys: dict,
) -> None:
    """
    MySQL 테이블 데이터를 읽어 Iceberg 테이블로 생성합니다.

    Args:
        spark (SparkSession): Spark 세션 객체
        config (Settings): 설정 정보 객체
        table_name (str): 대상 테이블 명 (db.table)
        jdbc_options (dict): JDBC 연결 옵션
        primary_keys (dict): 기본키 정보
        partition_keys (dict): 파티션 키 정보
    """
    logger = SparkLoggerManager().get_logger()

    # MySQL table name format (db.table) parsing
    parts = table_name.split(".")
    if len(parts) == 2:
        schema, table = parts
    else:
        raise ValueError(f"Invalid table name format: '{table_name}'. Expected 'db.table'.")

    bronze_schema = f"{schema.lower()}_bronze"
    target_table = table.lower()
    full_table_name = f"{config.CATALOG}.{bronze_schema}.{target_table}"

    pk_cols = primary_keys.get(table_name, [])
    partition_column = partition_keys.get(table_name)

    jdbc_df: DataFrame

    if partition_column:
        logger.info(f"Reading '{table_name}' with partitioning on column '{partition_column}'.")
        bound_query = f"SELECT min({partition_column}) as `lower`, max({partition_column}) as `upper` FROM {table_name}"
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
                .option("numPartitions", config.NUM_PARTITIONS)
                .load()
            )
    else:
        logger.info(f"Reading '{table_name}' without partitioning.")
        jdbc_df = spark.read.format("jdbc").options(**jdbc_options).option("dbtable", table_name).load()

    jdbc_df = cast_dataframe(jdbc_df)
    jdbc_df = jdbc_df.withColumn("last_applied_date", F.current_timestamp())

    # 데이터베이스 생성
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.CATALOG}.{bronze_schema}")

    logger.info(f"Creating or replacing {full_table_name}")

    if pk_cols:
        jdbc_df = jdbc_df.withColumn("id_iceberg", F.md5(F.concat_ws("|", *[F.col(pk) for pk in pk_cols])))

    # Iceberg 테이블 생성 및 데이터 쓰기 (RTAS)
    writer = (
        jdbc_df.writeTo(full_table_name)
        .using("iceberg")
        .tableProperty("location", f"{config.ICEBERG_S3_ROOT_PATH}/{bronze_schema}/{target_table}")
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


def main(spark: SparkSession, config: Settings) -> None:
    """
    Reads data from a MySQL database and saves it as Iceberg tables.
    """
    logger_manager = SparkLoggerManager()
    logger_manager.setup(spark)
    logger = logger_manager.get_logger()

    logger.info("Starting Iceberg table creation from MySQL.")
    logger.info(f"Target tables: {config.TABLE_LIST}")

    success_count = 0
    failed_tables = []
    total_tables = len(config.TABLE_LIST)

    # 데이터베이스별로 그룹화된 테이블 목록을 순회
    for database, table_list in config.TABLE_DICT.items():
        try:
            jdbc_options = get_jdbc_options(config, database)
            primary_keys = get_primary_keys(spark, config)
            partition_keys = get_partition_key_info(spark, config, database)

            for table_name in table_list:
                try:
                    process_mysql_to_iceberg(spark, config, table_name, jdbc_options, primary_keys, partition_keys)
                    success_count += 1
                    progress = (success_count / total_tables) * 100
                    logger.info(f"[{progress:3.1f}%] Successfully processed {success_count}/{total_tables} tables.")

                except Exception as e:
                    failed_tables.append({"table": table_name, "error": str(e)})
                    logger.error(f"[FAIL] Failed to process table '{table_name}': {e}")

        except Exception as db_e:
            logger.error(f"[FAIL] Critical error for database '{database}': {db_e}")
            failed_table_names = {d["table"] for d in failed_tables}
            for table_name in table_list:
                if table_name not in failed_table_names:
                    failed_tables.append({"table": table_name, "error": f"DB-level error: {db_e}"})

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
        SparkSession.builder.appName("MySQLToIceberg")
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
        .config("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.SimplifyCasts")
        .getOrCreate()
    )

    main(spark, settings)
    spark.stop()

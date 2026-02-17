import os

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

# --- Import common modules ---
from utils.database import get_jdbc_options, get_partition_key_info
from utils.settings import Settings
from utils.spark_logging import SparkLoggerManager


def process_table_to_parquet(
    spark: SparkSession, config: Settings, table_name: str, jdbc_options: dict, partition_keys: dict
) -> None:
    """
    특정 테이블의 데이터를 원본 DB에서 읽어 Parquet으로 저장합니다.

    Args:
        spark (SparkSession): Spark 세션 객체
        config (Settings): 설정 정보 객체
        table_name (str): 원본 테이블 명 (e.g., db.schema.table)
        jdbc_options (dict): JDBC 연결 옵션
        partition_keys (dict): 파티션 키 정보
    """
    logger = SparkLoggerManager().get_logger(__name__)

    # 테이블명 파싱 (MSSQL: db.schema.table, MySQL: db.table)
    parts = table_name.split(".")
    if len(parts) == 3:
        schema, _, table = parts
    else:
        schema, table = parts

    # Parquet 저장 경로 설정 ('dbo' 등 스키마는 경로에 포함)
    parquet_dir = f"{config.PARQUET_S3_ROOT_PATH}/{schema}/{table}"

    # 파티션 키 조회 (partition_keys 딕셔너리 키는 'schema.table' 형식)
    partition_column = partition_keys.get(f"{schema}.{table}")

    jdbc_df: DataFrame

    if partition_column:
        logger.info(f"Reading '{table_name}' with partitioning on column '{partition_column}'.")
        # 파티셔닝을 위한 min/max 값 조회 쿼리
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
                .option("numPartitions", config.NUM_PARTITIONS)
                .load()
            )
    else:
        logger.info(f"Reading '{table_name}' without partitioning.")
        jdbc_df = spark.read.format("jdbc").options(**jdbc_options).option("dbtable", table_name).load()

    # 데이터 처리 및 Parquet 저장
    (
        jdbc_df.withColumn(
            "update_ts_dms",
            F.date_format(F.from_utc_timestamp(F.current_timestamp(), "Asia/Seoul"), "yyyy-MM-dd HH:mm:ss.SSS"),
        )
        .write.mode("overwrite")
        .parquet(parquet_dir)
    )
    logger.info(f"Successfully wrote Parquet for '{table_name}' to '{parquet_dir}'")


def main(spark: SparkSession, config: Settings) -> None:
    """
    Reads data from source databases and saves it as Parquet files in S3.
    """
    logger_manager = SparkLoggerManager()
    logger_manager.setup(spark)
    logger = logger_manager.get_logger(__name__)

    logger.info("Starting Parquet conversion from source DB.")
    logger.info(f"Target tables: {config.TABLE_LIST}")

    failed_tables = []
    success_count = 0
    total_tables = len(config.TABLE_LIST)

    # 데이터베이스별로 루프를 돌며 처리
    for database, table_list in config.TABLE_DICT.items():
        try:
            # 각 데이터베이스에 맞는 JDBC 옵션과 파티션 키 정보 조회
            jdbc_options = get_jdbc_options(config, database)
            partition_keys = get_partition_key_info(spark, config, database)

            for table_name in table_list:
                try:
                    process_table_to_parquet(spark, config, table_name, jdbc_options, partition_keys)
                    success_count += 1
                    progress = (success_count / total_tables) * 100
                    logger.info(f"[{progress:3.1f}%] Successfully processed {success_count}/{total_tables} tables.")
                except Exception as e:
                    # 테이블 처리 실패 시 정보 기록
                    failed_tables.append({"table": table_name, "error": str(e)})
                    logger.error(f"[FAIL] Error processing table {table_name}: {e}")

        except Exception as db_e:
            # 데이터베이스 레벨에서 오류 발생 시, 해당 DB의 모든 테이블을 실패 처리
            logger.error(f"[FAIL] Critical error for database '{database}': {db_e}")
            failed_table_names = {d["table"] for d in failed_tables}
            for table_name in table_list:
                if table_name not in failed_table_names:
                    failed_tables.append({"table": table_name, "error": f"DB-level error: {db_e}"})

    # 최종 결과 요약 출력
    logger.info("--- Parquet Conversion Process Summary ---")
    logger.info(f"Total: {total_tables}, Success: {success_count}, Fail: {len(failed_tables)}")

    if failed_tables:
        for fail in failed_tables:
            logger.warn(f"Failed Table: {fail['table']} | Reason: {fail['error']}")
        raise RuntimeError(f"Process finished with {len(failed_tables)} failures.")

    logger.info("Parquet conversion process finished successfully.")


if __name__ == "__main__":
    settings = Settings()

    os.environ["AWS_PROFILE"] = settings.AWS_PROFILE

    spark = (
        SparkSession.builder.appName("sqlserver_to_parquet")
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
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )

    main(spark, settings)
    spark.stop()

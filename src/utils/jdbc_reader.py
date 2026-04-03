"""JDBC 테이블 읽기 공통 모듈

파티션 키 자동 탐색 → 경계값 조회 → 분산/단일 읽기를 하나의 함수로 제공한다.
"""

from pyspark.sql import DataFrame, SparkSession

from utils.database import BaseDatabaseManager
from utils.spark_logging import SparkLoggerManager


def read_jdbc_table(
    spark: SparkSession,
    db_manager: BaseDatabaseManager,
    table_name: str,
    num_partition: int,
    database: str = "",
) -> DataFrame:
    """JDBC로 테이블을 읽는다. 파티션 키가 존재하면 자동으로 분산 읽기를 수행한다.

    Args:
        spark: Spark 세션
        db_manager: DB 매니저 (MySQLManager 또는 SQLServerManager)
        table_name: 정규화된 테이블명 (MySQL: db.table, SQL Server: db.dbo.table)
        num_partition: 분산 읽기 시 파티션 수
        database: JDBC 연결에 사용할 데이터베이스명
    """
    logger = SparkLoggerManager().get_logger()

    partition_column = db_manager.get_partition_key(spark, table_name)
    jdbc_options = db_manager.get_jdbc_options(database=database)

    if not partition_column:
        logger.info(f"Reading '{table_name}' without partitioning.")
        return spark.read.format("jdbc").options(**jdbc_options).option("dbtable", table_name).load()

    logger.info(f"Reading '{table_name}' with partitioning on column '{partition_column}'.")

    # DB 종류에 따라 alias 따옴표 결정 (MySQL: 백틱, SQL Server: 싱글쿼트)
    q = "'" if db_manager.settings.database.type == "sqlserver" else "`"
    bound_query = (
        f"SELECT min({partition_column}) as {q}lower{q}, max({partition_column}) as {q}upper{q} FROM {table_name}"
    )
    bound_df = spark.read.format("jdbc").options(**jdbc_options).option("query", bound_query).load()
    bounds = bound_df.first()

    if not bounds or bounds["lower"] is None:
        logger.warn(
            f"Partition column '{partition_column}' has no data for table '{table_name}'. Reading without partitioning."
        )
        return spark.read.format("jdbc").options(**jdbc_options).option("dbtable", table_name).load()

    return (
        spark.read.format("jdbc")
        .options(**jdbc_options)
        .option("dbtable", table_name)
        .option("partitionColumn", partition_column)
        .option("lowerBound", bounds["lower"])
        .option("upperBound", bounds["upper"])
        .option("numPartitions", num_partition)
        .load()
    )

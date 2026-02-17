# src/utils/database.py
import re
from collections import OrderedDict
from enum import StrEnum
from textwrap import dedent

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession

from .settings import Settings


class DatabaseType(StrEnum):
    """
    Enum for supported database types.
    """

    MYSQL = "mysql"
    MSSQL = "mssql"


# --- Type Mappings ---
# 데이터베이스별 데이터 타입을 Spark 타입으로 매핑합니다.
# This dictionary maps database-specific data types to Spark's DataType.
MYSQL_TYPE_MAPPING = {
    "char": T.StringType(),
    "varchar": T.StringType(),
    "text": T.StringType(),
    "tinyint": T.IntegerType(),
    "smallint": T.IntegerType(),
    "int": T.IntegerType(),
    "int unsigned": T.LongType(),
    "bigint": T.LongType(),
    "float": T.DoubleType(),
    "double": T.DoubleType(),
    "decimal": lambda p, s: T.DecimalType(precision=int(p), scale=int(s)),
    "boolean": T.BooleanType(),
    "blob": T.BinaryType(),
    "time": T.TimestampType(),
    "date": T.DateType(),
    "datetime": T.TimestampType(),
    "timestamp": T.TimestampType(),
}
MSSQL_TYPE_MAPPING = {
    "bigint": T.LongType(),
    "int": T.IntegerType(),
    "smallint": T.ShortType(),
    "tinyint": T.ByteType(),
    "bit": T.BooleanType(),
    "decimal": T.DecimalType(38, 10),
    "numeric": T.DecimalType(38, 10),
    "money": T.DecimalType(19, 4),
    "smallmoney": T.DecimalType(10, 4),
    "float": T.DoubleType(),
    "real": T.FloatType(),
    "date": T.DateType(),
    "datetime": T.TimestampType(),
    "datetime2": T.TimestampType(),
    "smalldatetime": T.TimestampType(),
    "time": T.StringType(),
    "char": T.StringType(),
    "varchar": T.StringType(),
    "text": T.StringType(),
    "nchar": T.StringType(),
    "nvarchar": T.StringType(),
    "ntext": T.StringType(),
    "binary": T.BinaryType(),
    "varbinary": T.BinaryType(),
    "image": T.BinaryType(),
    "uniqueidentifier": T.StringType(),
}


def convert_db_type_to_spark(column_type: str, db_type: DatabaseType) -> T.DataType:
    """Converts a database-specific column type string to a Spark DataType."""
    type_map = MYSQL_TYPE_MAPPING if db_type == DatabaseType.MYSQL else MSSQL_TYPE_MAPPING
    type_name_match = re.match(r"^\w+", column_type.lower())

    if type_name_match:
        type_name = type_name_match.group(0)
        if type_name == "decimal" and db_type == DatabaseType.MYSQL:
            params = re.findall(r"\d+", column_type)
            if len(params) >= 2:
                return type_map[type_name](params[0], params[1])
            elif len(params) == 1:
                return type_map[type_name](params[0], 0)

        return type_map.get(type_name, T.StringType())

    return T.StringType()


# --- JDBC Utilities ---
def get_jdbc_options(config: Settings, database: str | None = None) -> dict[str, str]:
    """Generates JDBC connection options based on the database type in settings."""
    options: dict[str, str] = {}

    if config.DB_TYPE == DatabaseType.MYSQL:
        db_name = database if database else ""
        options["url"] = f"jdbc:mysql://{config.DB_HOST}:{config.DB_PORT}/{db_name}"
        options["driver"] = "com.mysql.cj.jdbc.Driver"
    elif config.DB_TYPE == DatabaseType.MSSQL:
        db_prop = f";databaseName={database}" if database else ""
        options["url"] = f"jdbc:sqlserver://{config.DB_HOST}:{config.DB_PORT}{db_prop};encrypt=false;"
        options["driver"] = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    else:
        raise ValueError(f"Unsupported database type: {config.DB_TYPE}")

    # 사용자 정보가 있을 경우에만 딕셔너리에 추가합니다.
    # Only add user and password to the dictionary if they exist.
    if config.DB_USER:
        options["user"] = config.DB_USER
    if config.DB_PASSWORD:
        options["password"] = config.DB_PASSWORD

    return options


def _execute_jdbc_query(spark: SparkSession, options: dict[str, str], query: str) -> DataFrame:
    """Executes a JDBC query and returns the result as a Spark DataFrame."""
    return spark.read.format("jdbc").options(**options).option("query", query).load()


# --- Metadata Retrieval ---
def get_primary_keys(spark: SparkSession, config: Settings) -> dict[str, list[str]]:
    """Retrieves primary key information for the specified tables."""
    if not config.TABLE_LIST:
        return {}

    db_name = config.TABLE_LIST[0].split(".")[0] if config.TABLE_LIST and config.DB_TYPE == DatabaseType.MSSQL else None

    if config.DB_TYPE == DatabaseType.MYSQL:
        query = dedent(f"""
            SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION
            FROM information_schema.COLUMNS
            WHERE CONCAT_WS('.', TABLE_SCHEMA, TABLE_NAME) IN {config.TABLE_STR} AND COLUMN_KEY = 'PRI'
        """)
    elif config.DB_TYPE == DatabaseType.MSSQL:
        # MSSQL의 경우, 각 데이터베이스 컨텍스트에서 쿼리를 실행해야 할 수 있습니다.
        # This query assumes it will be run in the context of the correct database for MSSQL.
        query = dedent(f"""
            SELECT t.TABLE_CATALOG AS TABLE_SCHEMA, t.TABLE_NAME, c.COLUMN_NAME, c.ORDINAL_POSITION
            FROM {db_name}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
            JOIN {db_name}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE c ON c.CONSTRAINT_NAME = t.CONSTRAINT_NAME
            WHERE t.CONSTRAINT_TYPE = 'PRIMARY KEY'
              AND CONCAT(t.TABLE_CATALOG, '.dbo.', t.TABLE_NAME) IN {config.TABLE_STR}
        """)
    else:
        raise ValueError(f"Unsupported database type: {config.DB_TYPE}")

    options = get_jdbc_options(config, db_name)
    df = _execute_jdbc_query(spark, options, query).orderBy("TABLE_SCHEMA", "TABLE_NAME", "ORDINAL_POSITION")

    pk_info = df.groupBy("TABLE_SCHEMA", "TABLE_NAME").agg(F.collect_list("COLUMN_NAME").alias("COLUMNS"))

    if config.DB_TYPE == DatabaseType.MSSQL:
        return {f"{row['TABLE_SCHEMA']}.dbo.{row['TABLE_NAME']}": row["COLUMNS"] for row in pk_info.collect()}

    return {f"{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}": row["COLUMNS"] for row in pk_info.collect()}


# get_table_schema_info, get_partition_key_info 같은 다른 유틸리티 함수들도 위와 유사한 패턴으로 추가될 수 있습니다.
# Additional utility functions like get_table_schema_info and get_partition_key_info can be added here
# following a similar pattern.


def get_table_schema_info(spark: SparkSession, config: Settings) -> dict[str, dict[str, str]]:
    """Retrieves column schema information for the specified tables."""
    if not config.TABLE_LIST:
        return {}

    db_name = config.TABLE_LIST[0].split(".")[0] if config.TABLE_LIST and config.DB_TYPE == DatabaseType.MSSQL else None

    if config.DB_TYPE == DatabaseType.MYSQL:
        query = dedent(f"""
            SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, ORDINAL_POSITION
            FROM information_schema.COLUMNS
            WHERE CONCAT_WS('.', TABLE_SCHEMA, TABLE_NAME) IN {config.TABLE_STR}
        """)
    elif config.DB_TYPE == DatabaseType.MSSQL:
        query = dedent(f"""
            SELECT TABLE_CATALOG AS TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE AS COLUMN_TYPE, ORDINAL_POSITION
            FROM {db_name}.INFORMATION_SCHEMA.COLUMNS
            WHERE CONCAT(TABLE_CATALOG, '.dbo.', TABLE_NAME) IN {config.TABLE_STR}
        """)
    else:
        raise ValueError(f"Unsupported database type: {config.DB_TYPE}")

    options = get_jdbc_options(config, db_name)
    df = _execute_jdbc_query(spark, options, query).orderBy("TABLE_SCHEMA", "TABLE_NAME", "ORDINAL_POSITION")

    # schema_info = (
    #     df.groupBy("TABLE_SCHEMA", "TABLE_NAME")
    #     .agg(F.collect_list(F.struct(F.col("COLUMN_NAME"), F.col("COLUMN_TYPE"))).alias("COLUMNS"))
    #     .select(F.col("TABLE_SCHEMA"), F.col("TABLE_NAME"), F.map_from_entries("COLUMNS").alias("COLUMNS"))
    # )

    # 테이블별 컬럼 dict 생성
    schema_info: dict[tuple[str, str], dict[str, str]] = {}
    for row in df.collect():
        table_name_tpl = (row.TABLE_SCHEMA, row.TABLE_NAME)
        if table_name_tpl not in schema_info:
            schema_info[table_name_tpl] = OrderedDict()
        schema_info[table_name_tpl][row.COLUMN_NAME] = row.COLUMN_TYPE

    # MSSQL의 경우 'dbo' 스키마를 이름에 포함시켜 일관성을 유지합니다.
    # For MSSQL, include the 'dbo' schema in the name for consistency.
    if config.DB_TYPE == DatabaseType.MSSQL:
        return {f"{schema}.dbo.{table}": cols for (schema, table), cols in schema_info.items()}

    return {f"{schema}.{table}": cols for (schema, table), cols in schema_info.items()}


def get_partition_key_info(spark: SparkSession, config: Settings, database: str | None = None) -> dict[str, str]:
    """
    지정된 테이블의 파티션 키를 조회합니다.
    DB 유형에 따라 다른 쿼리를 실행합니다.
    """
    if not config.TABLE_LIST:
        return {}

    jdbc_options = get_jdbc_options(config, database)
    table_str = (
        config.get_table_str_for_db(database) if database and config.DB_TYPE == DatabaseType.MSSQL else config.TABLE_STR
    )

    if config.DB_TYPE == DatabaseType.MYSQL:
        query = dedent(f"""
            SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS AS c
                 JOIN (SELECT TABLE_SCHEMA
                            , TABLE_NAME
                            , MIN(ORDINAL_POSITION)                                                       AS min_ordinal
                            , MIN(CASE WHEN EXTRA = 'auto_increment' THEN ORDINAL_POSITION ELSE NULL END) AS extra_ordinal
                       FROM INFORMATION_SCHEMA.COLUMNS
                       WHERE CONCAT_WS('.', TABLE_SCHEMA, TABLE_NAME) IN {table_str}
                              AND (DATA_TYPE IN ('int', 'bigint', 'date', 'datetime', 'timestamp') OR EXTRA LIKE 'auto_increment')
                       GROUP BY TABLE_SCHEMA, TABLE_NAME) AS t
                      ON c.TABLE_SCHEMA = t.TABLE_SCHEMA
                          AND c.TABLE_NAME = t.TABLE_NAME
                          AND (c.ORDINAL_POSITION = COALESCE(t.extra_ordinal, t.min_ordinal))
            ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
        """)
    elif config.DB_TYPE == DatabaseType.MSSQL:
        query = dedent(f"""
            SELECT t.DB_NAME AS TABLE_SCHEMA
                 , c.TABLE_NAME
                 , c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS AS c
                     JOIN (SELECT d.name                                                                   AS DB_NAME
                                , TABLE_SCHEMA
                                , TABLE_NAME
                                , MIN(ORDINAL_POSITION)                                                    AS min_ordinal
                                , MIN(CASE
                                          WHEN COLUMNPROPERTY(OBJECT_ID(CONCAT(TABLE_SCHEMA, '.', TABLE_NAME)), COLUMN_NAME,
                                                              'IsIdentity') = 1 THEN ORDINAL_POSITION END) AS extra_ordinal
                           FROM INFORMATION_SCHEMA.COLUMNS
                                    JOIN sys.databases AS d ON d.database_id = DB_ID()
                           WHERE (DATA_TYPE IN ('date', 'datetime', 'datetime2', 'timestamp') OR
                                  COLUMNPROPERTY(OBJECT_ID(CONCAT(TABLE_SCHEMA, '.', TABLE_NAME)), COLUMN_NAME, 'IsIdentity') = 1)
                             AND CONCAT(d.name, '.', TABLE_SCHEMA, '.', TABLE_NAME) IN {table_str}
                           GROUP BY D.name, TABLE_SCHEMA, TABLE_NAME) AS t
                          ON c.TABLE_SCHEMA = t.TABLE_SCHEMA
                              AND c.TABLE_NAME = t.TABLE_NAME
                              AND (c.ORDINAL_POSITION = COALESCE(t.extra_ordinal, t.min_ordinal))
            ORDER BY t.DB_NAME, c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
            OFFSET 0 ROWS
        """)
    else:
        raise ValueError(f"Unsupported database type: {config.DB_TYPE}")

    df = _execute_jdbc_query(spark, jdbc_options, query)
    return {f"{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}": row["COLUMN_NAME"] for row in df.collect()}

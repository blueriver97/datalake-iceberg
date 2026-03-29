import re
from abc import ABC, abstractmethod
from collections import OrderedDict
from textwrap import dedent

import pyspark.sql.functions as F  # noqa
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession

# --- Type Mappings ---
MYSQL_TYPE_MAPPING = {
    "char": T.StringType(),
    "varchar": T.StringType(),
    "text": T.StringType(),
    "tinytext": T.StringType(),
    "mediumtext": T.StringType(),
    "longtext": T.StringType(),
    "tinyint": T.IntegerType(),
    "smallint": T.IntegerType(),
    "mediumint": T.IntegerType(),
    "int": T.IntegerType(),
    "int unsigned": T.LongType(),
    "bigint": T.LongType(),
    "float": T.FloatType(),
    "double": T.DoubleType(),
    "decimal": lambda p, s: T.DecimalType(precision=int(p), scale=int(s)),
    "boolean": T.BooleanType(),
    "blob": T.BinaryType(),
    "tinyblob": T.BinaryType(),
    "mediumblob": T.BinaryType(),
    "longblob": T.BinaryType(),
    "time": T.TimestampType(),
    "date": T.DateType(),
    "datetime": T.TimestampType(),
    "timestamp": T.TimestampType(),
    "enum": T.StringType(),
    "set": T.StringType(),
    "json": T.StringType(),
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
    "xml": T.StringType(),
    "sql_variant": T.StringType(),
}


def convert_db_type_to_spark(column_type: str, db_type: str) -> T.DataType:
    """데이터베이스 컬럼 타입 문자열을 Spark DataType으로 변환합니다."""
    type_map = MYSQL_TYPE_MAPPING if db_type == "mysql" else MSSQL_TYPE_MAPPING
    type_name_match = re.match(r"^\w+(?:\s+\w+)?", column_type.lower().strip())

    if type_name_match:
        type_name = type_name_match.group(0)
        # MySQL tinyint(1) → BooleanType (JDBC 드라이버 동작과 일치)
        if type_name == "tinyint" and db_type == "mysql":
            params = re.findall(r"\d+", column_type)
            if params and params[0] == "1":
                return T.BooleanType()

        # MySQL decimal(p,s) 처리
        if type_name == "decimal" and db_type == "mysql":
            params = re.findall(r"\d+", column_type)
            if len(params) >= 2:
                return type_map[type_name](params[0], params[1])
            elif len(params) == 1:
                return type_map[type_name](params[0], 0)

        if type_name in type_map:
            return type_map[type_name]

        # "int unsigned" 같은 2단어 타입이 매칭 안 되면 첫 단어로 재시도
        first_word = type_name.split()[0] if " " in type_name else None
        if first_word and first_word in type_map:
            return type_map[first_word]

    return T.StringType()


class BaseDatabaseManager(ABC):
    """
    데이터베이스 관리를 위한 추상 베이스 클래스
    """

    def __init__(self, config):
        # 설정 객체 초기화
        self.config = config

    def _execute_jdbc_query(self, spark: SparkSession, options: dict[str, str], query: str) -> DataFrame:
        # JDBC 쿼리 실행 및 데이터프레임 반환
        print(f"Executing JDBC query on {self.config.database.type}")
        return spark.read.format("jdbc").options(**options).option("query", query).load()

    @abstractmethod
    def get_jdbc_options(self, database: str = "") -> dict[str, str]:
        # DB별 JDBC 옵션 생성
        pass

    @abstractmethod
    def get_primary_key(self, spark: SparkSession, table_name: str) -> list[str]:
        # PK 정보 조회
        pass

    @abstractmethod
    def get_partition_key(self, spark: SparkSession, table_name: str) -> str | None:
        # 파티션 키 후보 조회
        pass

    @abstractmethod
    def get_schema(self, spark: SparkSession, table_name: str) -> dict[str, str]:
        # 테이블 스키마 정보 조회
        pass

    @abstractmethod
    def get_metadata(self, spark: SparkSession, table_name: str) -> dict[str, str]:
        # 테이블 메타데이터 조회
        pass

    @abstractmethod
    def get_column_comments(self, spark: SparkSession, table_name: str) -> dict[str, str]:
        # 컬럼별 주석 조회 {column_name: comment}
        pass

    @abstractmethod
    def get_table_comment(self, spark: SparkSession, table_name: str) -> str | None:
        # 테이블 주석 조회
        pass

    @abstractmethod
    def get_nullable_info(self, spark: SparkSession, table_name: str) -> dict[str, bool]:
        # 컬럼별 nullable 정보 조회 {column_name: is_nullable}
        pass


class MySQLManager(BaseDatabaseManager):
    """
    MySQL 전용 관리 클래스
    """

    def get_jdbc_options(self, database: str = "") -> dict[str, str]:
        # MySQL 연결 옵션 생성
        db = self.config.database
        options = {
            "url": f"jdbc:mysql://{db.host}:{db.port}/{database}?zeroDateTimeBehavior=convertToNull&useUnicode=true&characterEncoding=UTF-8",
            "driver": "com.mysql.cj.jdbc.Driver",
            "user": db.user,
            "password": db.password.get_secret_value(),
        }
        return options

    def get_primary_key(self, spark: SparkSession, table_name: str) -> list[str]:
        options = self.get_jdbc_options()
        query = dedent(f"""
            SELECT COLUMN_NAME
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE CONCAT_WS('.', TABLE_SCHEMA, TABLE_NAME) = '{table_name}' AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
        """)
        df = self._execute_jdbc_query(spark, options, query)
        return [row.COLUMN_NAME for row in df.collect()]

    def get_partition_key(self, spark: SparkSession, table_name: str) -> str | None:
        # 분산 처리를 위한 파티션 키 자동 탐색 (Auto_increment 또는 숫자형 우선)
        options = self.get_jdbc_options()
        query = dedent(f"""
            SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS AS c
                 JOIN (SELECT TABLE_SCHEMA
                            , TABLE_NAME
                            , MIN(ORDINAL_POSITION)                                                       AS min_ordinal
                            , MIN(CASE WHEN EXTRA = 'auto_increment' THEN ORDINAL_POSITION ELSE NULL END) AS extra_ordinal
                       FROM INFORMATION_SCHEMA.COLUMNS
                       WHERE CONCAT_WS('.', TABLE_SCHEMA, TABLE_NAME) = '{table_name}'
                              AND (DATA_TYPE IN ('int', 'bigint', 'date', 'datetime', 'timestamp') OR EXTRA LIKE 'auto_increment')
                       GROUP BY TABLE_SCHEMA, TABLE_NAME) AS t
                      ON c.TABLE_SCHEMA = t.TABLE_SCHEMA
                          AND c.TABLE_NAME = t.TABLE_NAME
                          AND (c.ORDINAL_POSITION = COALESCE(t.extra_ordinal, t.min_ordinal))
            ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
        """)
        df = self._execute_jdbc_query(spark, options, query)
        row = df.first()
        return row.COLUMN_NAME if row else None

    def get_schema(self, spark: SparkSession, table_name: str) -> dict[str, str]:
        # Spark 데이터 타입 매핑을 포함한 스키마 조회
        options = self.get_jdbc_options()
        query = dedent(f"""
            SELECT COLUMN_NAME, COLUMN_TYPE
            FROM information_schema.COLUMNS
            WHERE CONCAT_WS('.', TABLE_SCHEMA, TABLE_NAME) = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """)
        df = self._execute_jdbc_query(spark, options, query)
        return OrderedDict([(row.COLUMN_NAME, row.COLUMN_TYPE) for row in df.collect()])

    def get_metadata(self, spark: SparkSession, table_name: str) -> dict[str, str]:
        options = self.get_jdbc_options()
        query = dedent(f"""
            SELECT TABLE_ROWS
                 , ROUND(((data_length + index_length) / 1024.0 / 1024.0), 0) AS 'TABLE_SIZE'
            FROM information_schema.TABLES
            WHERE CONCAT_WS('.', TABLE_SCHEMA, TABLE_NAME) = '{table_name}'
        """)
        df = self._execute_jdbc_query(spark, options, query)
        return {col.lower(): str(row[col]) for col in df.columns for row in df.collect()}

    def get_column_comments(self, spark: SparkSession, table_name: str) -> dict[str, str]:
        options = self.get_jdbc_options()
        query = dedent(f"""
            SELECT COLUMN_NAME, COLUMN_COMMENT
            FROM information_schema.COLUMNS
            WHERE CONCAT_WS('.', TABLE_SCHEMA, TABLE_NAME) = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """)
        df = self._execute_jdbc_query(spark, options, query)
        return {row.COLUMN_NAME: row.COLUMN_COMMENT for row in df.collect()}

    def get_table_comment(self, spark: SparkSession, table_name: str) -> str | None:
        options = self.get_jdbc_options()
        query = dedent(f"""
            SELECT TABLE_COMMENT
            FROM information_schema.TABLES
            WHERE CONCAT_WS('.', TABLE_SCHEMA, TABLE_NAME) = '{table_name}'
        """)
        df = self._execute_jdbc_query(spark, options, query)
        row = df.first()
        return row.TABLE_COMMENT if row and row.TABLE_COMMENT else None

    def get_nullable_info(self, spark: SparkSession, table_name: str) -> dict[str, bool]:
        options = self.get_jdbc_options()
        query = dedent(f"""
            SELECT COLUMN_NAME, IS_NULLABLE
            FROM information_schema.COLUMNS
            WHERE CONCAT_WS('.', TABLE_SCHEMA, TABLE_NAME) = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """)
        df = self._execute_jdbc_query(spark, options, query)
        return {row.COLUMN_NAME: row.IS_NULLABLE == "YES" for row in df.collect()}


class SQLServerManager(BaseDatabaseManager):
    """
    SQL Server 전용 관리 클래스
    """

    def get_jdbc_options(self, database: str | None = None) -> dict[str, str]:
        # MSSQL 연결 옵션 생성 (encrypt=false 기본값 설정)
        db = self.config.database
        db_prop = f";databaseName={database}" if database else ""
        options = {
            "url": f"jdbc:sqlserver://{db.host}:{db.port}{db_prop};encrypt=false;",
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "user": db.user,
            "password": db.password.get_secret_value(),
        }
        return options

    def get_primary_key(self, spark: SparkSession, table_name: str) -> list[str]:
        # MSSQL PK 조회 (Catalog.Schema.Table 형식 대응)
        db_name = table_name.split(".")[0]
        options = self.get_jdbc_options(db_name)
        query = dedent(f"""
            SELECT t.TABLE_CATALOG AS TABLE_SCHEMA, t.TABLE_NAME, c.COLUMN_NAME, c.ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE c ON c.CONSTRAINT_NAME = t.CONSTRAINT_NAME
            WHERE t.CONSTRAINT_TYPE = 'PRIMARY KEY'
              AND CONCAT(t.TABLE_CATALOG, '.dbo.', t.TABLE_NAME) = '{table_name}'
        """)
        df = self._execute_jdbc_query(spark, options, query)
        df = df.sort("TABLE_SCHEMA", "TABLE_NAME", "ORDINAL_POSITION")
        return [row.COLUMN_NAME for row in df.collect()]

    def get_partition_key(self, spark: SparkSession, table_name: str) -> str | None:
        # MSSQL IsIdentity 또는 날짜/숫자형 기반 파티션 키 탐색
        db_name = table_name.split(".")[0]
        options = self.get_jdbc_options(db_name)
        query = dedent(f"""
            SELECT TOP 1 c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS AS c
            WHERE CONCAT(TABLE_CATALOG, '.dbo.', TABLE_NAME) = '{table_name}'
              AND (DATA_TYPE IN ('date', 'datetime', 'datetime2', 'timestamp') OR
                   COLUMNPROPERTY(OBJECT_ID(CONCAT(TABLE_SCHEMA, '.', TABLE_NAME)), COLUMN_NAME, 'IsIdentity') = 1)
            ORDER BY (CASE WHEN COLUMNPROPERTY(OBJECT_ID(CONCAT(TABLE_SCHEMA, '.', TABLE_NAME)), COLUMN_NAME, 'IsIdentity') = 1 THEN 0 ELSE 1 END),
                     ORDINAL_POSITION
        """)
        df = self._execute_jdbc_query(spark, options, query)
        row = df.first()
        return row.COLUMN_NAME if row else None

    def get_schema(self, spark: SparkSession, table_name: str) -> dict[str, str]:
        # MSSQL 데이터 타입 조회 및 정렬
        db_name = table_name.split(".")[0]
        options = self.get_jdbc_options(db_name)
        query = dedent(f"""
            SELECT COLUMN_NAME, DATA_TYPE AS COLUMN_TYPE, ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE CONCAT(TABLE_CATALOG, '.dbo.', TABLE_NAME) = '{table_name}'
        """)
        df = self._execute_jdbc_query(spark, options, query)
        df = df.sort("ORDINAL_POSITION")
        return OrderedDict([(row.COLUMN_NAME, row.COLUMN_TYPE) for row in df.collect()])

    def get_metadata(self, spark: SparkSession, table_name: str) -> dict[str, str]:
        db_name, _, tbl = table_name.split(".")
        options = self.get_jdbc_options(db_name)
        query = dedent(f"""
            SELECT p.rows        AS TABLE_ROWS,
                   CAST(
                           (SUM(a.total_pages) * 8.0) / 1024
                       AS FLOAT) AS TABLE_SIZE
            FROM sys.tables AS t
                     INNER JOIN sys.indexes AS i
                                ON t.object_id = i.object_id
                                    AND t.object_id = OBJECT_ID(CONCAT('dbo.', '{tbl}'))
                     INNER JOIN sys.partitions AS p
                                ON i.object_id = p.object_id
                                    AND i.index_id = p.index_id
                     INNER JOIN sys.allocation_units AS a
                                ON p.partition_id = a.container_id
            GROUP BY t.name, p.rows
        """)
        df = self._execute_jdbc_query(spark, options, query)
        return {col.lower(): str(row[col]) for col in df.columns for row in df.collect()}

    def get_column_comments(self, spark: SparkSession, table_name: str) -> dict[str, str]:
        db_name, _, tbl = table_name.split(".")
        options = self.get_jdbc_options(db_name)
        query = dedent(f"""
            SELECT c.name AS COLUMN_NAME,
                   CAST(ep.value AS NVARCHAR(4000)) AS COLUMN_COMMENT
            FROM sys.columns c
                INNER JOIN sys.tables t ON c.object_id = t.object_id
                LEFT JOIN sys.extended_properties ep
                    ON ep.major_id = c.object_id
                    AND ep.minor_id = c.column_id
                    AND ep.name = 'MS_Description'
            WHERE t.name = '{tbl}'
            ORDER BY c.column_id
        """)
        df = self._execute_jdbc_query(spark, options, query)
        return {row.COLUMN_NAME: (row.COLUMN_COMMENT or "") for row in df.collect()}

    def get_table_comment(self, spark: SparkSession, table_name: str) -> str | None:
        db_name, _, tbl = table_name.split(".")
        options = self.get_jdbc_options(db_name)
        query = dedent(f"""
            SELECT CAST(ep.value AS NVARCHAR(4000)) AS TABLE_COMMENT
            FROM sys.tables t
                INNER JOIN sys.extended_properties ep
                    ON ep.major_id = t.object_id
                    AND ep.minor_id = 0
                    AND ep.name = 'MS_Description'
            WHERE t.name = '{tbl}'
        """)
        df = self._execute_jdbc_query(spark, options, query)
        row = df.first()
        return row.TABLE_COMMENT if row and row.TABLE_COMMENT else None

    def get_nullable_info(self, spark: SparkSession, table_name: str) -> dict[str, bool]:
        db_name = table_name.split(".")[0]
        options = self.get_jdbc_options(db_name)
        query = dedent(f"""
            SELECT COLUMN_NAME, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE CONCAT(TABLE_CATALOG, '.dbo.', TABLE_NAME) = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """)
        df = self._execute_jdbc_query(spark, options, query)
        return {row.COLUMN_NAME: row.IS_NULLABLE == "YES" for row in df.collect()}

# src/utils/settings.py
from collections import defaultdict
from enum import StrEnum

import hvac
from pydantic import Field
from pydantic_settings import BaseSettings


class DatabaseType(StrEnum):
    """
    Enum for supported database types.
    """

    MYSQL = "mysql"
    MSSQL = "mssql"


class Settings(BaseSettings):
    """
    Manages all configuration settings for the project.
    Loads environment variables from a specified .env file.
    """

    # Note. Vault Settings
    VAULT_URL: str = Field(..., description="URL for HashiCorp Vault")
    VAULT_USERNAME: str = Field(..., description="Username for Vault authentication")
    VAULT_PASSWORD: str = Field(..., description="Password for Vault authentication")
    VAULT_SECRET_PATH: str = Field(..., description="Path to the secret in Vault")

    # Note. Database Settings
    DB_TYPE: DatabaseType = Field(DatabaseType.MYSQL, description="Type of the database (e.g., mysql, mssql)")
    DB_HOST: str | None = Field(None, description="Database host address")
    DB_PORT: int | None = Field(None, description="Database port number")
    DB_USER: str | None = Field(None, description="Database username")
    DB_PASSWORD: str | None = Field(None, description="Database password")

    # Note. AWS, Spark, and Iceberg Settings
    AWS_PROFILE: str = Field(default="", description="AWS profile name to use")
    CATALOG: str = Field(..., description="Name of the Iceberg catalog")
    ICEBERG_S3_ROOT_PATH: str = Field(..., description="Root S3 path for Iceberg tables")
    PARQUET_S3_ROOT_PATH: str | None = Field(None, description="Root S3 path for Parquet files")

    # Note. Kafka Settings
    BOOTSTRAP_SERVERS: str | None = Field(None, description="Kafka bootstrap servers")
    SCHEMA_REGISTRY: str | None = Field(None, description="URL for the Schema Registry")
    TOPIC_PREFIX: str | None = Field(None, description="Prefix for Kafka topics")
    METRIC_NAMESPACE: str | None = Field(None, description="Namespace for metrics")
    MAX_OFFSETS_PER_TRIGGER: int = Field(100_000, description="Maximum number of offsets to process per trigger")
    STARTING_OFFSETS: str = Field("earliest", description="The starting position for Kafka consumption")

    # Note. Job-specific Settings
    TABLES: str = Field("", description="Comma-separated list of tables to process (<schema>.<table>)")
    SCHEMAS: str = Field("", description="Comma-separated list of schemas to process")
    NUM_PARTITIONS: int = Field(20, description="Number of partitions for JDBC parallel reads")

    class Config:
        extra = "ignore"
        use_enum_values = True  # Enum 값을 문자열로 사용

    @property
    def TABLE_LIST(self) -> list[str]:
        """Converts the TABLES string to a list of table names."""
        return [table.strip() for table in self.TABLES.split(",") if table.strip()]

    @property
    def TABLE_STR(self) -> str:
        """Creates a tuple-like string of table names for use in SQL IN clauses."""
        if not self.TABLE_LIST:
            return "('')"
        # 튜플에 요소가 하나일 때 발생하는 후행 쉼표(trailing comma) 문제를 해결합니다.
        # Handles the trailing comma issue for single-element tuples.
        return f"{tuple(self.TABLE_LIST)}".replace(",)", ")")

    @property
    def TABLE_DICT(self) -> dict:
        """TABLES 문자열을 데이터베이스별 테이블 목록 딕셔너리로 변환합니다."""
        table_dict = defaultdict(list)
        for table in self.TABLE_LIST:
            schema = table.split(".", 1)[0]
            table_dict[schema].append(table)
        return table_dict

    @property
    def TOPIC_LIST(self) -> list:
        return [f"{self.TOPIC_PREFIX}.{table}" for table in self.TABLE_LIST]

    @property
    def TOPIC_DICT(self) -> dict:
        return {table: f"{self.TOPIC_PREFIX}.{table}" for table in self.TABLE_LIST}

    @property
    def SCHEMA_LIST(self) -> list[str]:
        """Converts the SCHEMAS string to a list of schema names."""
        return [schema.strip() for schema in self.SCHEMAS.split(",") if schema.strip()]

    @property
    def SCHEMA_STR(self) -> str:
        schema_list = self.SCHEMAS.split(",") if self.SCHEMAS else []
        return f"{tuple(map(str, schema_list))}".replace(",)", ")")

    @property
    def CHECKPOINT_LOCATION(self) -> str:
        return f"{self.ICEBERG_S3_ROOT_PATH}/checkpoint/{self.METRIC_NAMESPACE}"

    def get_table_str_for_db(self, database: str) -> str:
        """특정 데이터베이스에 대한 테이블 목록을 SQL IN 절 형식의 문자열로 반환합니다."""
        table_list = self.TABLE_DICT.get(database, [])
        if not table_list:
            return "('')"
        return f"{tuple(map(str.strip, table_list))}".replace(",)", ")")

    def init_vault(self, localhost: bool = False) -> None:
        """Initializes the Vault client and loads database credentials."""
        try:
            client = hvac.Client(url=self.VAULT_URL)
            client.auth.userpass.login(username=self.VAULT_USERNAME, password=self.VAULT_PASSWORD)
            response = client.read(self.VAULT_SECRET_PATH)

            if not isinstance(response, dict) or "data" not in response or "data" not in response["data"]:
                raise ValueError(f"Could not find data at Vault path: '{self.VAULT_SECRET_PATH}'")

            secret = response["data"]["data"]
            self.DB_HOST = secret.get("host") if not localhost else "localhost"
            self.DB_PORT = int(secret.get("port", 0))
            self.DB_USER = secret.get("user") or secret.get("username")
            self.DB_PASSWORD = secret.get("password")
        except Exception as e:
            # 로거가 설정되기 전 단계일 수 있으므로 print를 사용합니다.
            # Using print as the logger may not be configured at this stage.
            print(f"An error occurred during Vault initialization: {e}")
            raise

from typing import Any

import hvac
from pydantic import BaseModel, Field, SecretStr
from pydantic.fields import FieldInfo
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict


class VaultSettings(BaseModel):
    url: str = Field(description="Vault URL")
    username: str = Field(description="Vault username")
    password: SecretStr = Field(description="Vault password")
    secret_path: str = Field(description="Vault secret path")


class DatabaseSettings(BaseModel):
    type: str = Field(description="Database type (mysql, sqlserver)")
    host: str = Field(default="", description="Database host address")
    port: int = Field(default=3306, description="Database port number")
    user: str = Field(default="", description="Database username")
    password: SecretStr = Field(default=SecretStr(""), description="Database password")


class AwsSettings(BaseModel):
    profile: str = Field(description="AWS profile name")
    catalog: str = Field(description="Iceberg catalog name")
    s3_bucket: str = Field(description="S3 bucket name")
    iceberg_path: str = Field(description="Iceberg table path")
    parquet_path: str = Field(default="/data/raw", description="Raw Parquet storage path")


class KafkaSettings(BaseModel):
    bootstrap_servers: str = Field(description="Kafka bootstrap servers")
    schema_registry: str = Field(description="Kafka schema registry URL")
    topic_prefix: str = Field(description="Kafka topic prefix")
    metric_namespace: str = Field(description="Kafka metric namespace")
    max_offsets_per_trigger: int = Field(description="Maximum number of offsets to fetch per trigger")
    starting_offsets: str = Field(description="Kafka starting offsets (earliest, latest)")


class VaultSettingsSource(PydanticBaseSettingsSource):
    def __init__(self, settings_cls: type[BaseSettings], vault_config: dict[str, Any]):
        super().__init__(settings_cls)
        self.vault_config = vault_config

    def __call__(self, *args, **kwargs):
        url = self.vault_config.get("url")
        username = self.vault_config.get("username")
        password = self.vault_config.get("password")
        secret_path = self.vault_config.get("secret_path")

        if not all([url, username, password, secret_path]):
            raise ValueError("from env: url, username, password, secret_path must be set")

        try:
            client = hvac.Client(url=url)
            client.auth.userpass.login(username=username, password=password)
            response = client.read(path=secret_path)

            if not isinstance(response, dict) or "data" not in response or "data" not in response["data"]:
                raise ValueError(f"Could not find data at Vault path: '{secret_path}'")

            secret = response["data"]["data"]
            return {
                "database": {
                    "host": secret.get("host"),
                    "port": int(secret.get("port", 0)),
                    "user": secret.get("user"),
                    "password": secret.get("password"),
                }
            }
        except Exception as e:
            print(f"An error occurred during Vault initialization: {e}")
            return {}

    def get_field_value(self, field: FieldInfo, field_name: str) -> tuple[Any, str, bool]:
        return super().get_field_value(field, field_name)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore", env_file=".env", env_file_encoding="utf-8", env_nested_delimiter="__"
    )
    vault: VaultSettings
    database: DatabaseSettings
    aws: AwsSettings
    kafka: KafkaSettings | None = None

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        env_data = env_settings()
        env_data.update(dotenv_settings())
        vault_config = env_data.get("vault", {})
        vault_settings = VaultSettingsSource(settings_cls, vault_config)
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            vault_settings,
            file_secret_settings,
        )

    @property
    def CATALOG(self):
        return self.aws.catalog

    @property
    def WAREHOUSE(self):
        return "s3a://" + self.aws.s3_bucket + self.aws.iceberg_path

    @property
    def PARQUET_WAREHOUSE(self):
        return "s3a://" + self.aws.s3_bucket + self.aws.parquet_path


# if __name__ == "__main__":
#     settings = Settings(_env_file="../../tests/test.env")
#     print(settings)

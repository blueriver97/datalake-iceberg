import os
from pathlib import Path
from typing import Any

import hvac
import yaml
from pydantic import BaseModel, Field, SecretStr
from pydantic.fields import FieldInfo
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource


class YamlConfigSettingsSource(PydanticBaseSettingsSource):
    def __init__(self, settings_cls: type[BaseSettings], yaml_path: str | None = None):
        super().__init__(settings_cls)
        # 경로가 지정되지 않으면 기본값 사용
        self.yaml_path = Path(__file__).parent.parent / "settings.yml"
        if yaml_path:
            self.yaml_path = Path(yaml_path)

    def __call__(self) -> dict[str, Any]:
        # 파일 존재 여부 확인
        if not self.yaml_path.exists():
            print(f"Settings file not found at {self.yaml_path}")
            return {}

        try:
            # 파일 읽기 및 로드
            with open(self.yaml_path, encoding="utf-8") as f:
                data = yaml.safe_load(f)
                return data if data else {}
        except yaml.YAMLError as e:
            print(f"Failed to parse YAML file at {self.yaml_path}: {e}")
            raise
        except Exception as e:
            print(f"Unexpected error loading settings from {self.yaml_path}: {e}")
            raise

    def get_field_value(self, field: FieldInfo, field_name: str) -> tuple[Any, str, bool]:
        return super().get_field_value(field, field_name)


class VaultConfigSettingsSource(PydanticBaseSettingsSource):
    def __init__(self, settings_cls: type[BaseSettings], yaml_data: dict):
        super().__init__(settings_cls)
        self.yaml_data = yaml_data

    def __call__(self, *args, **kwargs) -> dict[str, Any]:
        try:
            vault_config = self.yaml_data.get("vault", {})
            vault_url = vault_config.get("url")
            vault_username = vault_config.get("username")
            vault_password = vault_config.get("password")
            vault_secret_path = vault_config.get("secret_path")

            client = hvac.Client(url=vault_url)
            client.auth.userpass.login(username=vault_username, password=vault_password)
            response = client.read(vault_secret_path)

            if not isinstance(response, dict) or "data" not in response or "data" not in response["data"]:
                raise ValueError(f"Could not find data at Vault path: '{vault_secret_path}'")

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


class VaultSettings(BaseModel):
    # 민감 정보 노출 방지를 위해 SecretStr 사용
    url: str
    username: str
    password: SecretStr
    secret_path: str


class DatabaseSettings(BaseModel):
    host: str = Field(default="localhost", description="Database host address")
    port: int = Field(default=3306, description="Database port number")
    user: str = Field(default="root", description="Database username")
    password: SecretStr = Field(default="", description="Database password")


class AwsSettings(BaseModel):
    profile: str


class IcebergSettings(BaseModel):
    catalog: str
    s3_root_path: str
    parquet_s3_root_path: str


class KafkaSettings(BaseModel):
    bootstrap_servers: str
    schema_registry: str
    topic_prefix: str
    metric_namespace: str
    max_offsets_per_trigger: int = Field(default=100000)
    starting_offsets: str


class JobSettings(BaseModel):
    num_partitions: int = Field(default=20)
    tables: list[str]


class Settings(BaseSettings):
    __config_path__: str | None = os.getenv("CONFIG_PATH", None)

    vault: VaultSettings
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    aws: AwsSettings
    iceberg: IcebergSettings
    kafka: KafkaSettings
    job: JobSettings

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # 1. YAML 소스 인스턴스 생성 및 데이터 미리 로드
        yaml_settings = YamlConfigSettingsSource(settings_cls, cls.__config_path__)
        yaml_data = yaml_settings()  # YAML 파일 읽기 실행

        # 2. 로드된 데이터를 Vault 소스에 전달
        vault_settings = VaultConfigSettingsSource(settings_cls, yaml_data)
        return (
            init_settings,
            yaml_settings,
            vault_settings,
            env_settings,
            dotenv_settings,
            file_secret_settings,
        )


# if __name__ == "__main__":
#     settings = Settings()
#     print(settings)

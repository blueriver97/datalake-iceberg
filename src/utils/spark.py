"""Spark 인프라 모듈 — SparkSession 팩토리 + Log4j 로거 매니저

모든 Spark 앱이 공유하는 Glue Catalog / S3 / Iceberg 설정과
Log4j 기반 로깅을 중앙 관리한다.
"""

import threading

from pyspark.sql import SparkSession

from utils.settings import Settings

# ---------------------------------------------------------------------------
# SparkSession Factory
# ---------------------------------------------------------------------------


def create_spark_session(
    app_name: str,
    settings: Settings,
    extra_configs: dict[str, str] | None = None,
) -> SparkSession:
    """공통 설정이 적용된 SparkSession을 생성한다.

    포함 설정:
        - Glue Data Catalog (SparkCatalog)
        - S3FileIO + DefaultCredentialsProvider
        - IcebergSparkSessionExtensions
        - session.timeZone = UTC

    Args:
        app_name: Spark 앱 이름
        settings: 환경 설정 객체
        extra_configs: 앱별 추가 Spark 설정 (key-value)
    """
    catalog = settings.CATALOG

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.defaultCatalog", catalog)
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{catalog}.warehouse", settings.WAREHOUSE)
        .config(f"spark.sql.catalog.{catalog}.s3.path-style-access", "true")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
        )
        .config("spark.sql.session.timeZone", "UTC")
    )

    if extra_configs:
        for key, value in extra_configs.items():
            builder = builder.config(key, value)

    return builder.getOrCreate()


# ---------------------------------------------------------------------------
# Log4j Logger Manager
# ---------------------------------------------------------------------------


class SparkLoggerManager:
    _instance = None
    _lock = threading.Lock()
    _initialized = False
    _log_manager = None  # JVM LogManager 저장용

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def setup(self, spark):
        """Spark JVM을 통한 Log4j 초기화 및 매니저 로드"""
        if self._initialized:
            return

        with self._lock:
            if self._initialized:
                return

            try:
                jvm = spark._jvm
                self._log_manager = jvm.org.apache.logging.log4j.LogManager
                configurator = jvm.org.apache.logging.log4j.core.config.Configurator
                level = jvm.org.apache.logging.log4j.Level

                # 로그 레벨 설정
                configurator.setLevel("org.apache.spark", level.INFO)

                self._initialized = True
                print("SparkLoggerManager: Log4j 2 has been initialized.")
            except Exception as e:
                print(f"Error: Failed to setup Log4j 2. Detail: {str(e)}")

    def get_logger(self, name: str = ""):
        """JVM 로거 객체 반환"""
        if not self._initialized or self._log_manager is None:
            print("Warning: SparkLoggerManager not initialized. Call setup(spark) first.")
            return None

        # JVM의 getLogger 호출
        return self._log_manager.getLogger("org.apache.spark." + name if name else "org.apache.spark")

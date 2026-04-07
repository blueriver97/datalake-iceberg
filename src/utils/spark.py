"""Spark 인프라 모듈 — SparkSession 팩토리 + Log4j 로거 매니저

모든 Spark 앱이 공유하는 Iceberg 카탈로그(Glue / Polaris) 설정과
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

    카탈로그 타입(glue / polaris)에 따라 적절한 설정을 적용한다.

    Args:
        app_name: Spark 앱 이름
        settings: 환경 설정 객체
        extra_configs: 앱별 추가 Spark 설정 (key-value)
    """
    catalog = settings.CATALOG

    # spark.sql.catalog.{catalog}: catalog은 Spark 내부 로컬 별칭이며 임의 값 가능.
    # 실제 서버 연결 정보는 하위 설정(.warehouse, .uri 등)으로 결정된다.
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.defaultCatalog", catalog)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.session.timeZone", "UTC")
    )

    if settings.storage.catalog_type == "polaris":
        # Polaris REST Catalog: OAuth2 인증, warehouse는 Polaris 서버에 등록된 카탈로그 이름
        if settings.polaris is None:
            raise ValueError("catalog_type='polaris' requires POLARIS__* settings")
        polaris = settings.polaris
        builder = (
            builder.config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{catalog}.type", "rest")
            # Polaris 서버에 등록된 카탈로그 이름 (논리적 식별자, 스토리지 경로 아님)
            .config(f"spark.sql.catalog.{catalog}.warehouse", catalog)
            .config(f"spark.sql.catalog.{catalog}.uri", polaris.uri)
            .config(f"spark.sql.catalog.{catalog}.oauth2-server-uri", polaris.oauth2_server_uri)
            .config(f"spark.sql.catalog.{catalog}.header.Polaris-Realm", polaris.realm)
            .config(
                f"spark.sql.catalog.{catalog}.header.X-Iceberg-Access-Delegation",
                "vended-credentials",
            )
            .config(f"spark.sql.catalog.{catalog}.credential", polaris.credential)
            .config(f"spark.sql.catalog.{catalog}.scope", polaris.scope)
            .config(f"spark.sql.catalog.{catalog}.token-refresh-enabled", "true")
        )
    else:
        # Glue Data Catalog: AWS 자격증명 기반, warehouse는 S3 물리 경로
        builder = (
            builder.config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            # Iceberg 테이블의 실제 S3 저장 경로 (e.g. s3a://bucket/iceberg)
            .config(f"spark.sql.catalog.{catalog}.warehouse", settings.WAREHOUSE)
            .config(f"spark.sql.catalog.{catalog}.s3.path-style-access", "true")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
            )
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

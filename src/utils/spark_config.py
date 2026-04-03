"""SparkSession 공통 설정 모듈

모든 Spark 앱이 공유하는 Glue Catalog / S3 / Iceberg 설정을 중앙 관리한다.
"""

from pyspark.sql import SparkSession

from utils.settings import Settings


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

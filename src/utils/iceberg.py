"""
Iceberg 테이블 생성 + 데이터 정제 유틸리티
"""

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession

from utils.settings import Settings
from utils.spark import SparkLoggerManager

# ---------------------------------------------------------------------------
# Data Cleansing
# ---------------------------------------------------------------------------


def trim_string_columns(df: DataFrame) -> DataFrame:
    """StringType 컬럼의 앞뒤 공백을 제거한다.

    JDBC로 읽은 CHAR 타입 컬럼에 패딩된 공백을 정리하는 데 사용한다.
    """
    return df.select(
        [
            F.trim(F.col(field.name)).alias(field.name)
            if isinstance(field.dataType, T.StringType)
            else F.col(field.name).alias(field.name)
            for field in df.schema.fields
        ]
    )


# ---------------------------------------------------------------------------
# Iceberg Table Writer
# ---------------------------------------------------------------------------


def create_or_replace_iceberg_table(
    spark: SparkSession,
    df: DataFrame,
    settings: Settings,
    bronze_schema: str,
    target_table: str,
    pk_cols: list[str] | None = None,
) -> None:
    """Iceberg 테이블을 생성하거나 교체한다 (RTAS).

    1. 대상 데이터베이스가 없으면 생성
    2. format-version 2로 Iceberg 테이블 생성/교체
    3. PK가 있으면 메타데이터 정리 속성 추가

    Args:
        spark: SparkSession
        df: 쓸 DataFrame (id_iceberg 컬럼이 이미 추가된 상태)
        settings: Settings (CATALOG, WAREHOUSE)
        bronze_schema: 대상 스키마 (예: store_bronze)
        target_table: 대상 테이블 (예: orders)
        pk_cols: PK 컬럼 목록 (None이면 메타데이터 정리 속성 미적용)
    """
    logger = SparkLoggerManager().get_logger()
    full_table_name = f"{settings.CATALOG}.{bronze_schema}.{target_table}"

    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {settings.CATALOG}.{bronze_schema} LOCATION '{settings.WAREHOUSE}/{bronze_schema}'"
    )

    logger.info(f"Creating or replacing {full_table_name}")

    writer = (
        df.writeTo(full_table_name)
        .using("iceberg")
        .tableProperty("location", f"{settings.WAREHOUSE}/{bronze_schema}/{target_table}")
        .tableProperty("format-version", "2")
    )

    # accept-any-schema: 쓰기 시 스키마 불일치를 허용하여 스키마 진화를 자동 적용한다.
    # 단, MERGE INTO와 호환되지 않음 — target에만 존재하는 컬럼을 source에서 resolve할 수 없어
    # UNRESOLVED_COLUMN.WITH_SUGGESTION 오류가 발생한다. append/overwrite 전용.
    writer = writer.tableProperty("write.spark.accept-any-schema", "false")

    # Merge On Read 활성화 시 아래 옵션 추가
    # writer = (
    #     writer.tableProperty("write.delete.mode", "merge-on-read")
    #     .tableProperty("write.update.mode", "merge-on-read")
    #     .tableProperty("write.merge.mode", "merge-on-read")
    # )

    if pk_cols:
        writer = (
            writer.tableProperty("write.metadata.delete-after-commit.enabled", "true")
            .tableProperty("write.metadata.previous-versions-max", "5")
            .tableProperty("history.expire.max-snapshot-age-ms", "86400000")
            # .partitionedBy(F.bucket(num_partition, "id_iceberg"))
        )

    writer.createOrReplace()
    logger.info(f"Successfully created or replaced {full_table_name}")

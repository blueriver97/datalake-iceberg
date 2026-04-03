"""
Iceberg 테이블 스키마 검증 및 주석 동기화

소스 DB 스키마와 Iceberg 테이블의 컬럼 수/순서/타입/nullable/PK를 비교하고,
테이블·컬럼 주석을 소스 DB에서 Iceberg로 동기화한다.

실행:
  spark-submit --py-files utils.zip schema_validate.py \
    --table "db.table_name" --env-file .env
"""

import argparse

from pyspark.sql import SparkSession

# --- Import common modules ---
from utils.database import BaseDatabaseManager, MySQLManager, SQLServerManager, convert_db_type_to_spark
from utils.settings import Settings
from utils.spark_config import create_spark_session
from utils.spark_logging import SparkLoggerManager

# Iceberg가 자동 추가하는 메타 컬럼 (비교 대상에서 제외)
ICEBERG_META_COLUMNS = {"last_applied_date", "id_iceberg"}


def _parse_table_name(table_name: str, db_type: str) -> tuple[str, str]:
    """테이블명 파싱하여 (bronze_schema, target_table) 반환"""
    parts = table_name.split(".")
    if db_type == "sqlserver" and len(parts) == 3:
        schema, _, table = parts
    elif len(parts) == 2:
        schema, table = parts
    else:
        raise ValueError(f"Invalid table name format: '{table_name}'.")
    return f"{schema.lower()}_bronze", table.lower()


def compare_columns(
    spark: SparkSession,
    settings: Settings,
    db_manager: BaseDatabaseManager,
    table_name: str,
) -> None:
    """컬럼 수, 순서, 데이터 타입 비교 + 미반영 컬럼 리포팅"""
    logger = SparkLoggerManager().get_logger()
    bronze_schema, target_table = _parse_table_name(table_name, settings.database.type)
    full_table_name = f"{settings.CATALOG}.{bronze_schema}.{target_table}"

    source_schema = db_manager.get_schema(spark, table_name)
    iceberg_schema = spark.table(full_table_name).schema

    source_cols = list(source_schema.keys())
    iceberg_cols = [f.name for f in iceberg_schema.fields if f.name not in ICEBERG_META_COLUMNS]

    # 컬럼 수 비교
    if len(source_cols) != len(iceberg_cols):
        logger.warn(f"[{table_name}] Column count mismatch: source={len(source_cols)}, iceberg={len(iceberg_cols)}")
    else:
        logger.info(f"[{table_name}] Column count match: {len(source_cols)}")

    # 미반영 컬럼 (원천에 있으나 Iceberg에 없는 컬럼)
    iceberg_col_set = {c.lower() for c in iceberg_cols}
    unreflected = [c for c in source_cols if c.lower() not in iceberg_col_set]
    if unreflected:
        logger.warn(f"[{table_name}] Unreflected columns (in source but not in Iceberg): {unreflected}")

    # 컬럼 순서 비교
    matched_source = [c for c in source_cols if c.lower() in iceberg_col_set]
    matched_iceberg = [c for c in iceberg_cols if c.lower() in {s.lower() for s in source_cols}]
    if [c.lower() for c in matched_source] != [c.lower() for c in matched_iceberg]:
        logger.warn(f"[{table_name}] Column order mismatch")

    # 데이터 타입 비교
    iceberg_field_map = {f.name.lower(): f for f in iceberg_schema.fields if f.name not in ICEBERG_META_COLUMNS}
    for col_name, source_type in source_schema.items():
        iceberg_field = iceberg_field_map.get(col_name.lower())
        if iceberg_field is None:
            continue
        expected_type = convert_db_type_to_spark(source_type, settings.database.type)
        if not isinstance(iceberg_field.dataType, type(expected_type)):
            logger.warn(
                f"[{table_name}] Type mismatch for '{col_name}': "
                f"source={source_type} -> expected={expected_type}, actual={iceberg_field.dataType}"
            )


def sync_column_comments(
    spark: SparkSession,
    settings: Settings,
    db_manager: BaseDatabaseManager,
    table_name: str,
) -> None:
    """원천 DB 컬럼 주석을 Iceberg 테이블에 동기화"""
    logger = SparkLoggerManager().get_logger()
    bronze_schema, target_table = _parse_table_name(table_name, settings.database.type)
    full_table_name = f"{settings.CATALOG}.{bronze_schema}.{target_table}"

    source_comments = db_manager.get_column_comments(spark, table_name)
    iceberg_fields = {f.name.lower(): f for f in spark.table(full_table_name).schema.fields}

    synced = 0
    for col_name, comment in source_comments.items():
        if not comment or col_name.lower() not in iceberg_fields:
            continue
        # Iceberg 기존 주석과 비교하여 다른 경우에만 동기화
        iceberg_comment = iceberg_fields[col_name.lower()].metadata.get("comment", "")
        if iceberg_comment == comment:
            continue
        escaped = comment.replace("'", "\\'")
        spark.sql(f"ALTER TABLE {full_table_name} ALTER COLUMN `{col_name}` COMMENT '{escaped}'")
        synced += 1

    logger.info(f"[{table_name}] Synced {synced} column comment(s)")


def compare_nullable(
    spark: SparkSession,
    settings: Settings,
    db_manager: BaseDatabaseManager,
    table_name: str,
) -> None:
    """원천 DB와 Iceberg 테이블의 nullable 정합성 비교"""
    logger = SparkLoggerManager().get_logger()
    bronze_schema, target_table = _parse_table_name(table_name, settings.database.type)
    full_table_name = f"{settings.CATALOG}.{bronze_schema}.{target_table}"

    source_nullable = db_manager.get_nullable_info(spark, table_name)
    iceberg_schema = spark.table(full_table_name).schema
    iceberg_field_map = {f.name.lower(): f for f in iceberg_schema.fields if f.name not in ICEBERG_META_COLUMNS}

    mismatches = []
    for col_name, is_nullable in source_nullable.items():
        iceberg_field = iceberg_field_map.get(col_name.lower())
        if iceberg_field is None:
            continue
        if iceberg_field.nullable != is_nullable:
            mismatches.append(f"{col_name}(source={is_nullable}, iceberg={iceberg_field.nullable})")

    if mismatches:
        logger.warn(f"[{table_name}] Nullable mismatches: {', '.join(mismatches)}")
    else:
        logger.info(f"[{table_name}] Nullable check passed")


def compare_primary_keys(
    spark: SparkSession,
    settings: Settings,
    db_manager: BaseDatabaseManager,
    table_name: str,
) -> None:
    """원천 DB PK와 Iceberg identifier fields 비교"""
    logger = SparkLoggerManager().get_logger()
    bronze_schema, target_table = _parse_table_name(table_name, settings.database.type)
    full_table_name = f"{settings.CATALOG}.{bronze_schema}.{target_table}"

    source_pks = db_manager.get_primary_key(spark, table_name)

    # Iceberg에서 id_iceberg가 존재하면 PK 기반 해시키가 설정된 것
    iceberg_cols = {f.name for f in spark.table(full_table_name).schema.fields}
    has_id_iceberg = "id_iceberg" in iceberg_cols

    if source_pks and not has_id_iceberg:
        logger.warn(f"[{table_name}] Source has PK {source_pks} but Iceberg has no id_iceberg column")
    elif not source_pks and has_id_iceberg:
        logger.warn(f"[{table_name}] Source has no PK but Iceberg has id_iceberg column")
    else:
        logger.info(f"[{table_name}] PK check passed (source_pks={source_pks}, id_iceberg={has_id_iceberg})")


def sync_table_comment(
    spark: SparkSession,
    settings: Settings,
    db_manager: BaseDatabaseManager,
    table_name: str,
) -> None:
    """원천 DB 테이블 주석을 Iceberg 테이블에 동기화"""
    logger = SparkLoggerManager().get_logger()
    bronze_schema, target_table = _parse_table_name(table_name, settings.database.type)
    full_table_name = f"{settings.CATALOG}.{bronze_schema}.{target_table}"

    comment = db_manager.get_table_comment(spark, table_name)
    if not comment:
        logger.info(f"[{table_name}] No table comment to sync")
        return

    # Iceberg 기존 테이블 주석과 비교 (Glue Catalog은 comment를 Description 필드에 저장)
    desc_df = spark.sql(f"DESCRIBE TABLE EXTENDED {full_table_name}").filter("col_name = 'Comment'")
    iceberg_comment = ""
    if not desc_df.isEmpty():
        row = desc_df.first()
        if row and row.data_type:
            iceberg_comment = row.data_type
    if iceberg_comment == comment:
        logger.info(f"[{table_name}] Table comment unchanged, skipping")
        return

    escaped = comment.replace("'", "\\'")
    spark.sql(f"ALTER TABLE {full_table_name} SET TBLPROPERTIES ('comment' = '{escaped}')")
    logger.info(f"[{table_name}] Table comment synced: '{comment}'")


def process_schema_validate(
    spark: SparkSession,
    settings: Settings,
    db_manager: BaseDatabaseManager,
    table_name: str,
) -> None:
    """테이블에 대해 5개 스키마 검증/동기화 작업을 수행합니다."""
    logger = SparkLoggerManager().get_logger()
    logger.info(f"Starting schema validation for {table_name}")

    compare_columns(spark, settings, db_manager, table_name)
    sync_column_comments(spark, settings, db_manager, table_name)
    compare_nullable(spark, settings, db_manager, table_name)
    compare_primary_keys(spark, settings, db_manager, table_name)
    sync_table_comment(spark, settings, db_manager, table_name)

    logger.info(f"Schema validation completed for {table_name}")


def main(spark: SparkSession, settings: Settings, app_args) -> None:
    """
    Validates source database schema against Iceberg and syncs comments.
    """
    logger_manager = SparkLoggerManager()
    logger_manager.setup(spark)
    logger = logger_manager.get_logger()

    logger.info("Starting schema validation.")

    table_name = app_args.table

    try:
        db_manager: BaseDatabaseManager
        if settings.database.type == "sqlserver":
            db_manager = SQLServerManager(settings)
        else:
            db_manager = MySQLManager(settings)

        process_schema_validate(spark, settings, db_manager, table_name)
    except Exception as e:
        logger.error(f"Failed to validate schema for '{table_name}': {e}")
        raise e
    else:
        logger.info("Schema validation process finished successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", type=str)
    parser.add_argument("--env-file", type=str, default=".env", help="환경 설정 파일 경로 (기본값: .env)")
    args = parser.parse_args()
    settings = Settings(_env_file=args.env_file)

    spark = create_spark_session("schema_validate", settings)

    main(spark, settings, args)
    spark.stop()

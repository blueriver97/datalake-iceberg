"""
Kafka CDC → Iceberg 파이프라인 공통 모듈

kafka_to_iceberg.py (배치) / kafka_to_iceberg_stream.py (상시 실행)에서 공유하는
Debezium 스키마 파싱, 타입 캐스팅, 배치 처리, 스트림 실행 로직.
"""

import json
import time
from dataclasses import dataclass
from textwrap import dedent

import pyspark.sql.functions as F
import pyspark.sql.types as T
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql.avro.functions import from_avro

from utils.maintenance import ProcessedTableTracker, run_position_delete_compaction
from utils.settings import Settings
from utils.spark import SparkLoggerManager
from utils.watermark import append_cdc_watermark, should_run

# ---------------------------------------------------------------------------
# Debezium Schema / Type Casting
# ---------------------------------------------------------------------------


def extract_debezium_schema(schema: dict) -> dict:
    """Debezium 메시지 스키마에서 {컬럼명: 커넥터타입} 딕셔너리를 추출한다."""
    debezium_type = {}
    envelope_fields = schema.get("fields", [])
    value_schema = None
    for field in envelope_fields:
        if field.get("name") in ("before", "after"):
            for type_def in field.get("type", []):
                if isinstance(type_def, dict) and "fields" in type_def:
                    value_schema = type_def
                    break
            if value_schema:
                break

    if not value_schema:
        return {}

    for col_field in value_schema.get("fields", []):
        col_name = col_field.get("name")
        if not col_name:
            continue

        type_info = col_field.get("type")
        actual_type_def = None
        if isinstance(type_info, list):
            for item in type_info:
                if item != "null":
                    actual_type_def = item
                    break
        else:
            actual_type_def = type_info

        if not actual_type_def:
            continue

        if isinstance(actual_type_def, dict):
            dbz_type = actual_type_def.get("connect.name") or actual_type_def.get("type")
        elif isinstance(actual_type_def, str):
            dbz_type = actual_type_def
        else:
            dbz_type = None

        if dbz_type:
            debezium_type[col_name] = dbz_type

    return debezium_type


def cast_column(column: Column, debezium_dtype: str) -> Column:
    """Debezium CDC 메시지의 데이터 타입을 Spark/Iceberg 타입으로 변환합니다.

    Debezium은 MySQL의 날짜/시간 타입을 다음과 같이 변환합니다:
    1. DATE 타입: 1970-01-01로부터의 일수 (정수)
       예: "date1": {"int": 18641} -> 1970-01-01부터 18641일 후

    2. TIME 타입: 자정으로부터의 마이크로초 (long)
       예: "time1": {"long": 18291000000} -> 00:00:00으로부터 18291000000 마이크로초 후

    3. DATETIME 타입: Unix epoch부터의 밀리초 (long)
       예: "datetime1": {"long": 1758712669000} -> 1970-01-01 00:00:00 UTC부터 1758712669000 밀리초 후

    4. DATETIME(6) 타입: Unix epoch부터의 마이크로초 (long)
       예: "create_datetime": {"long": 1758712669557813} -> 1970-01-01 00:00:00 UTC부터 1758712669557813 마이크로초 후

    5. TIMESTAMP 타입: ISO-8601 형식의 문자열
       예: "update_timestamp": {"string": "2025-09-24T02:17:49.557813Z"}

    Args:
        column: 변환할 Spark Column 객체
        debezium_dtype: Debezium 커넥터가 사용하는 데이터 타입 문자열

    Returns:
        변환된 Spark Column 객체

    Note:
        Avro 스키마의 default: 0 때문에 강제 주입된 값을 걸러냅니다.
        column.isNotNull() 만으로는 부족하며, 반드시 (column != 0) 체크가 병행되어야 합니다.
    """
    if debezium_dtype == "io.debezium.time.Date":
        return F.date_add(F.lit("1970-01-01"), column.cast("int"))
    elif debezium_dtype == "io.debezium.time.MicroTime":
        return F.to_utc_timestamp(F.timestamp_seconds(column / 1_000_000), "UTC")
    elif debezium_dtype == "io.debezium.time.Timestamp":
        is_valid = column.isNotNull() & (column != 0)
        return F.when(is_valid, F.to_utc_timestamp(F.timestamp_millis(column), "Asia/Seoul")).otherwise(
            F.lit(None).cast(T.TimestampType())
        )
    elif debezium_dtype == "io.debezium.time.MicroTimestamp":
        is_valid = column.isNotNull() & (column != 0)
        return F.when(is_valid, F.to_utc_timestamp(F.timestamp_micros(column), "Asia/Seoul")).otherwise(
            F.lit(None).cast(T.TimestampType())
        )
    elif debezium_dtype == "io.debezium.time.ZonedTimestamp":
        pass
    return column


# ---------------------------------------------------------------------------
# Pipeline Context
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PipelineContext:
    """배치마다 바뀌지 않는 파이프라인 실행 컨텍스트."""

    spark: SparkSession
    settings: Settings
    schema_registry_client: SchemaRegistryClient
    topic: str
    dag_id: str
    scheduled_at: str | None = None
    tracker: ProcessedTableTracker | None = None
    position_delete_interval: int = 0
    position_delete_last_map: dict[str, object] | None = None


# ---------------------------------------------------------------------------
# Batch Processing
# ---------------------------------------------------------------------------


def _transform_and_dedup(
    spark: SparkSession,
    schema_filtered_df: DataFrame,
    key_schema_str: str,
    value_schema_str: str,
    debezium_schema: dict,
    pk_cols: list[str],
    full_table_name: str,
) -> tuple[DataFrame, DataFrame] | None:
    """Avro 역직렬화 → Debezium 타입 캐스팅 → 중복 제거 → (upsert_df, delete_df) 반환.

    Iceberg 테이블이 존재하지 않으면 None을 반환한다.
    """
    logger = SparkLoggerManager().get_logger()

    transformed_df = (
        schema_filtered_df.withColumn("key", from_avro(F.col("key"), key_schema_str, {"mode": "FAILFAST"}))
        .withColumn("value", from_avro(F.col("value"), value_schema_str, {"mode": "FAILFAST"}))
        .withColumn(
            "id_iceberg",
            F.md5(F.concat_ws("|", *[cast_column(F.col(f"key.{c}"), debezium_schema.get(c, "")) for c in pk_cols])),
        )
        .select(
            "value.after.*",
            F.col("value.op").alias("__op"),
            F.col("offset").alias("__offset"),
            F.timestamp_millis(F.col("value.ts_ms")).alias("last_applied_date"),
            "id_iceberg",
        )
    )

    try:
        catalog_schema = spark.table(full_table_name).schema
    except Exception:
        logger.warn(f"Table {full_table_name} not found. Skipping.")
        return None

    cdc_df = transformed_df.select(
        *[
            cast_column(F.col(f.name), debezium_schema.get(f.name, "")).cast(f.dataType).alias(f.name)
            for f in catalog_schema.fields
        ],
        "__op",
        "__offset",
    )

    window_spec = Window.partitionBy("id_iceberg").orderBy(F.desc("__offset"))
    dedup_df = (
        cdc_df.withColumn("__row", F.row_number().over(window_spec))
        .filter(F.col("__row") == 1)
        .drop("__row", "__offset")
    )

    upsert_df = dedup_df.filter(F.col("__op") != "d").drop("__op")
    delete_df = dedup_df.filter(F.col("__op") == "d").drop("__op")
    return upsert_df, delete_df


def _apply_cdc_changes(
    spark: SparkSession,
    full_table_name: str,
    view_suffix: str,
    upsert_df: DataFrame,
    delete_df: DataFrame,
) -> None:
    """upsert_df에 대해 MERGE INTO, delete_df에 대해 DELETE를 실행한다."""
    logger = SparkLoggerManager().get_logger()

    if not upsert_df.isEmpty():
        view_name = f"upsert_view_{view_suffix}"
        upsert_df.createOrReplaceGlobalTempView(view_name)
        columns = upsert_df.columns
        update_expr = ", ".join([f"t.{c} = s.{c}" for c in columns])
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join([f"s.{c}" for c in columns])
        logger.info(f"Executing Merge Into for {full_table_name}")
        spark.sql(
            dedent(f"""
            MERGE INTO {full_table_name} t
            USING (SELECT * FROM global_temp.{view_name}) s
            ON t.id_iceberg = s.id_iceberg
            WHEN MATCHED THEN UPDATE SET {update_expr}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """)
        )

    if not delete_df.isEmpty():
        view_name = f"delete_view_{view_suffix}"
        delete_df.createOrReplaceGlobalTempView(view_name)
        logger.info(f"Executing Delete for {full_table_name}")
        spark.sql(
            dedent(f"""
            DELETE FROM {full_table_name} t
            WHERE EXISTS (
                SELECT s.id_iceberg FROM global_temp.{view_name} s
                WHERE s.id_iceberg = t.id_iceberg
            )
        """)
        )


def process_batch(batch_df: DataFrame, batch_id: int, ctx: PipelineContext) -> None:
    spark = ctx.spark
    logger = SparkLoggerManager().get_logger()
    table_start_time = time.monotonic()

    _prefix, schema, table = ctx.topic.split(".")
    iceberg_schema = f"{schema.lower()}_bronze"
    iceberg_table = table.lower()
    full_table_name = f"{ctx.settings.CATALOG}.{iceberg_schema}.{iceberg_table}"

    logger.info(f"<batch-{batch_id}> Processing {ctx.topic}")

    batch_df.persist(StorageLevel.MEMORY_AND_DISK)

    # 스키마 레지스트리 조회
    value_schema_ids = [row.value_schema_id for row in batch_df.select("value_schema_id").distinct().collect()]
    value_schema_dict = {sid: ctx.schema_registry_client.get_schema(sid).schema_str for sid in value_schema_ids}
    key_schema_ids = [row.key_schema_id for row in batch_df.select("key_schema_id").distinct().collect()]
    key_schema_dict = {sid: ctx.schema_registry_client.get_schema(sid).schema_str for sid in key_schema_ids}

    logger.info(f"{ctx.topic} | Key Schema Ids: {key_schema_ids} | Value Schema Ids: {value_schema_ids}")

    # 스키마 버전별 데이터 변환 및 Iceberg 적재
    # 스키마 ID 오름차순 정렬: 구 버전을 먼저 처리하여 신 버전 MERGE가 최종 상태를 보장한다.
    for value_schema_id, value_schema_str in sorted(value_schema_dict.items()):
        schema_filtered_df = batch_df.filter(F.col("value_schema_id") == value_schema_id)
        value_schema = json.loads(value_schema_str)
        debezium_schema = extract_debezium_schema(value_schema)

        current_key_schema_rows = schema_filtered_df.select("key_schema_id").distinct().collect()
        if not current_key_schema_rows:
            continue

        key_schema_id = current_key_schema_rows[0].key_schema_id
        key_schema_str = key_schema_dict.get(key_schema_id)
        if not key_schema_str:
            logger.warn(f"Key schema not found for id {key_schema_id}")
            continue

        key_schema = json.loads(key_schema_str)
        pk_cols = [field["name"] for field in key_schema["fields"]]

        result = _transform_and_dedup(
            spark,
            schema_filtered_df,
            key_schema_str,
            value_schema_str,
            debezium_schema,
            pk_cols,
            full_table_name,
        )
        if result is None:
            continue

        upsert_df, delete_df = result
        _apply_cdc_changes(spark, full_table_name, table, upsert_df, delete_df)

    # 수정된 테이블 추적
    if ctx.tracker is not None:
        ctx.tracker.mark(full_table_name)

    # Watermark 기록
    table_duration = time.monotonic() - table_start_time
    stats = batch_df.agg(
        F.count("*").alias("cnt"),
        F.date_format(F.max("timestamp"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("max_ts"),
        F.min("offset").alias("min_offset"),
        F.max("offset").alias("max_offset"),
    ).collect()[0]

    batch_df.unpersist()

    append_cdc_watermark(
        spark,
        ctx.settings.CATALOG,
        ctx.dag_id,
        iceberg_schema,
        iceberg_table,
        event_count=stats["cnt"],
        max_event_ts=stats["max_ts"],
        min_offset=stats["min_offset"],
        max_offset=stats["max_offset"],
        batch_id=batch_id,
        processing_duration_sec=table_duration,
        scheduled_at=ctx.scheduled_at,
    )


# ---------------------------------------------------------------------------
# Stream Runner
# ---------------------------------------------------------------------------


def run_topic_stream(
    spark: SparkSession,
    settings: Settings,
    topic: str,
    dag_id: str,
    starting_offsets: str | None = None,
    scheduled_at: str | None = None,
    tracker: ProcessedTableTracker | None = None,
    position_delete_interval: int = 0,
    position_delete_last_map: dict | None = None,
) -> None:
    logger = SparkLoggerManager().get_logger()

    if not settings.kafka:
        raise ValueError("Kafka configuration is missing.")

    _prefix, schema, table = topic.split(".")
    iceberg_schema = f"{schema.lower()}_bronze"
    iceberg_table = table.lower()

    checkpoint_path = f"s3a://{settings.storage.bucket}/iceberg/checkpoint/{dag_id}/{topic}"
    logger.info(f"Starting stream for topic: {topic}, checkpoint: {checkpoint_path}")

    ctx = PipelineContext(
        spark=spark,
        settings=settings,
        schema_registry_client=SchemaRegistryClient({"url": settings.kafka.schema_registry}),
        topic=topic,
        dag_id=dag_id,
        scheduled_at=scheduled_at,
        tracker=tracker,
        position_delete_interval=position_delete_interval,
        position_delete_last_map=position_delete_last_map,
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", topic)
        .option("maxOffsetsPerTrigger", settings.kafka.max_offsets_per_trigger)
        .option(
            "startingOffsets",
            json.dumps({topic: json.loads(starting_offsets)}) if starting_offsets else settings.kafka.starting_offsets,
        )
        .option("failOnDataLoss", "false")
        .load()
    )

    processed = False

    def _foreach_batch(batch_df: DataFrame, batch_id: int) -> None:
        """클로저: 외부 스코프의 ctx를 캡처하여 배치 처리에 전달한다."""
        nonlocal processed
        process_batch(batch_df, batch_id, ctx)
        processed = True

    query = (
        kafka_df.withColumn("key_schema_id", F.expr("byte_to_int(substring(key, 2, 4))"))
        .withColumn("key", F.expr("substring(key, 6, length(key)-5)"))
        .withColumn("value_schema_id", F.expr("byte_to_int(substring(value, 2, 4))"))
        .withColumn("value", F.expr("substring(value, 6, length(value)-5)"))
        .selectExpr("key_schema_id", "value_schema_id", "key", "value", "topic", "offset", "timestamp")
        .writeStream.foreachBatch(_foreach_batch)
        .option("checkpointLocation", checkpoint_path)
        .queryName(topic)
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    # position delete compaction (MoR 전용, pre-fetched map으로 시간 판단)
    if processed and position_delete_interval > 0 and position_delete_last_map is not None:
        key = f"{iceberg_schema}.{iceberg_table}"
        if should_run(position_delete_last_map.get(key), position_delete_interval):
            full_table_name = f"{settings.CATALOG}.{iceberg_schema}.{iceberg_table}"
            run_position_delete_compaction(spark, settings.CATALOG, dag_id, full_table_name)

    # availableNow=True에서 새 메시지가 없으면 foreachBatch가 호출되지 않음.
    # 파이프라인 활성 상태 추적을 위해 heartbeat watermark를 기록한다.
    if not processed:
        append_cdc_watermark(
            spark,
            settings.CATALOG,
            dag_id,
            iceberg_schema,
            iceberg_table,
            event_count=0,
            max_event_ts=None,
            scheduled_at=scheduled_at,
        )

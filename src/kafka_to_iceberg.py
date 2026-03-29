"""
Kafka CDC → Iceberg Pipeline (토픽별 독립 스트림, 멀티스레드 병렬 처리)

Airflow에서 spark-submit으로 실행:
  spark-submit --py-files utils.zip kafka_to_iceberg.py --topics "prefix.schema.table1,prefix.schema.table2"

S3 시그널 파일로 중단:
  s3a://{bucket}/spark/signal/{dag_id} 파일이 존재하면 남은 토픽 처리를 건너뛴다.
"""

import json
import sys
import threading
import time
from argparse import ArgumentParser
from dataclasses import dataclass
from textwrap import dedent

import pyspark.sql.functions as F
import pyspark.sql.types as T
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark import InheritableThread, StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql.avro.functions import from_avro

from utils.listener import BatchProgressListener
from utils.settings import Settings
from utils.signal import build_signal_path, check_stop_signal, cleanup_stop_signal
from utils.spark_logging import SparkLoggerManager

# ---------------------------------------------------------------------------
# Watermark
# ---------------------------------------------------------------------------


def ensure_watermark_table(spark: SparkSession, config: Settings) -> None:
    """ops_bronze.cdc_watermark 테이블이 없으면 생성한다."""
    logger = SparkLoggerManager().get_logger()
    full_table_name = f"{config.CATALOG}.ops_bronze.cdc_watermark"
    if not spark.catalog.tableExists(full_table_name):
        logger.info(f"Creating watermark table: {full_table_name}")
        spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS {config.CATALOG}.ops_bronze
            LOCATION '{config.WAREHOUSE}/ops_bronze'
        """)
        spark.sql(f"""
            CREATE TABLE {full_table_name} (
                dag_id                  STRING,
                bronze_schema           STRING,
                table_name              STRING,
                event_count             BIGINT,
                max_event_ts            TIMESTAMP,
                processed_at            TIMESTAMP,
                min_offset              BIGINT,
                max_offset              BIGINT,
                batch_id                BIGINT,
                processing_duration_sec DOUBLE
            ) USING iceberg
            LOCATION '{config.WAREHOUSE}/ops_bronze/cdc_watermark'
            TBLPROPERTIES (
                'format-version' = '2',
                'write.metadata.delete-after-commit.enabled' = 'true',
                'write.metadata.previous-versions-max' = '5'
            )
        """)


def _build_watermark_values(
    dag_id: str,
    bronze_schema: str,
    table_name: str,
    event_count: int,
    max_event_ts,
    min_offset: int | None,
    max_offset: int | None,
    batch_id: int | None,
    processing_duration_sec: float | None,
) -> str:
    """watermark INSERT/MERGE에 사용할 SELECT 절을 생성한다."""
    max_event_ts_expr = f"TIMESTAMP '{max_event_ts}'" if max_event_ts else "CAST(NULL AS TIMESTAMP)"
    min_offset_expr = str(min_offset) if min_offset is not None else "CAST(NULL AS BIGINT)"
    max_offset_expr = str(max_offset) if max_offset is not None else "CAST(NULL AS BIGINT)"
    batch_id_expr = str(batch_id) if batch_id is not None else "CAST(NULL AS BIGINT)"
    duration_expr = str(processing_duration_sec) if processing_duration_sec is not None else "CAST(NULL AS DOUBLE)"
    return f"""
        SELECT
            '{dag_id}' AS dag_id,
            '{bronze_schema}' AS bronze_schema,
            '{table_name}' AS table_name,
            {event_count} AS event_count,
            {max_event_ts_expr} AS max_event_ts,
            current_timestamp() AS processed_at,
            {min_offset_expr} AS min_offset,
            {max_offset_expr} AS max_offset,
            {batch_id_expr} AS batch_id,
            {duration_expr} AS processing_duration_sec
    """


def _log_watermark(
    bronze_schema, table_name, event_count, max_event_ts, min_offset, max_offset, processing_duration_sec
):
    logger = SparkLoggerManager().get_logger()
    if processing_duration_sec is not None:
        logger.info(
            f"watermark: {bronze_schema}.{table_name}, "
            f"events={event_count}, max_ts={max_event_ts}, "
            f"offsets=[{min_offset}, {max_offset}], duration={processing_duration_sec:.1f}s"
        )
    else:
        logger.info(f"watermark: {bronze_schema}.{table_name}, events={event_count}, max_ts={max_event_ts}")


def append_watermark(
    spark: SparkSession,
    config: Settings,
    dag_id: str,
    bronze_schema: str,
    table_name: str,
    event_count: int,
    max_event_ts,
    min_offset: int | None = None,
    max_offset: int | None = None,
    batch_id: int | None = None,
    processing_duration_sec: float | None = None,
) -> None:
    """cdc_watermark 테이블에 CDC 처리 기록을 append한다.

    동시 쓰기 시 Iceberg 스냅샷 충돌이 발생하지 않는다.
    최신 상태 조회 시 (dag_id, bronze_schema, table_name) 기준
    processed_at DESC로 첫 번째 행을 사용한다.
    """
    values = _build_watermark_values(
        dag_id,
        bronze_schema,
        table_name,
        event_count,
        max_event_ts,
        min_offset,
        max_offset,
        batch_id,
        processing_duration_sec,
    )
    spark.sql(f"INSERT INTO {config.CATALOG}.ops_bronze.cdc_watermark {values}")
    _log_watermark(
        bronze_schema, table_name, event_count, max_event_ts, min_offset, max_offset, processing_duration_sec
    )


def merge_watermark(
    spark: SparkSession,
    config: Settings,
    dag_id: str,
    bronze_schema: str,
    table_name: str,
    event_count: int,
    max_event_ts,
    min_offset: int | None = None,
    max_offset: int | None = None,
    batch_id: int | None = None,
    processing_duration_sec: float | None = None,
) -> None:
    """cdc_watermark 테이블에 CDC 처리 기록을 upsert한다.

    단일 writer 환경에서만 사용한다. 동시 쓰기 시 Iceberg 스냅샷 충돌이
    발생할 수 있으므로, 멀티스레드/멀티프로세스 환경에서는 append_watermark를 사용한다.
    """
    full_table = f"{config.CATALOG}.ops_bronze.cdc_watermark"
    values = _build_watermark_values(
        dag_id,
        bronze_schema,
        table_name,
        event_count,
        max_event_ts,
        min_offset,
        max_offset,
        batch_id,
        processing_duration_sec,
    )
    spark.sql(f"""
        MERGE INTO {full_table} t
        USING ({values}) s
        ON t.dag_id = s.dag_id
           AND t.bronze_schema = s.bronze_schema
           AND t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET
            t.event_count = s.event_count,
            t.max_event_ts = s.max_event_ts,
            t.processed_at = s.processed_at,
            t.min_offset = s.min_offset,
            t.max_offset = s.max_offset,
            t.batch_id = s.batch_id,
            t.processing_duration_sec = s.processing_duration_sec
        WHEN NOT MATCHED THEN INSERT *
    """)
    _log_watermark(
        bronze_schema, table_name, event_count, max_event_ts, min_offset, max_offset, processing_duration_sec
    )


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
    elif debezium_dtype in ("io.debezium.time.Timestamp", "io.debezium.time.MicroTimestamp"):
        is_valid = column.isNotNull() & (column != 0)
        return F.when(is_valid, F.to_utc_timestamp(F.timestamp_millis(column), "Asia/Seoul")).otherwise(
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
    config: Settings
    schema_registry_client: SchemaRegistryClient
    topic: str
    dag_id: str


# ---------------------------------------------------------------------------
# Batch Processing
# ---------------------------------------------------------------------------


def process_batch(batch_df: DataFrame, batch_id: int, ctx: PipelineContext) -> None:
    spark = ctx.spark
    logger = SparkLoggerManager().get_logger()
    table_start_time = time.monotonic()

    _prefix, schema, table = ctx.topic.split(".")
    iceberg_schema = f"{schema.lower()}_bronze"
    iceberg_table = table.lower()
    full_table_name = f"{ctx.config.CATALOG}.{iceberg_schema}.{iceberg_table}"

    logger.info(f"<batch-{batch_id}> Processing {ctx.topic}")

    if batch_df.isEmpty():
        append_watermark(
            spark,
            ctx.config,
            ctx.dag_id,
            iceberg_schema,
            iceberg_table,
            event_count=0,
            max_event_ts=None,
            batch_id=batch_id,
        )
        return

    batch_df.persist(StorageLevel.MEMORY_AND_DISK)

    # 스키마 레지스트리 조회
    value_schema_ids = [row.value_schema_id for row in batch_df.select("value_schema_id").distinct().collect()]
    value_schema_dict = {sid: ctx.schema_registry_client.get_schema(sid).schema_str for sid in value_schema_ids}
    key_schema_ids = [row.key_schema_id for row in batch_df.select("key_schema_id").distinct().collect()]
    key_schema_dict = {sid: ctx.schema_registry_client.get_schema(sid).schema_str for sid in key_schema_ids}

    logger.info(f"{ctx.topic} | Key Schema Ids: {key_schema_ids} | Value Schema Ids: {value_schema_ids}")

    # 스키마 버전별 데이터 변환 및 Iceberg 적재
    for value_schema_id, value_schema_str in value_schema_dict.items():
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

        # Avro 역직렬화 및 변환
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

        # Iceberg 테이블 스키마 조회 및 캐스팅
        try:
            catalog_schema = spark.table(full_table_name).schema
        except Exception:
            logger.warn(f"Table {full_table_name} not found. Skipping.")
            continue

        cdc_df = transformed_df.select(
            *[
                cast_column(F.col(f.name), debezium_schema.get(f.name, "")).cast(f.dataType).alias(f.name)
                for f in catalog_schema.fields
            ],
            "__op",
            "__offset",
        )

        # 중복 제거 (동일 키에 대해 최신 오프셋만 유지)
        window_spec = Window.partitionBy("id_iceberg").orderBy(F.desc("__offset"))
        dedup_df = (
            cdc_df.withColumn("__row", F.row_number().over(window_spec))
            .filter(F.col("__row") == 1)
            .drop("__row", "__offset")
        )

        upsert_df = dedup_df.filter(F.col("__op") != "d").drop("__op")
        delete_df = dedup_df.filter(F.col("__op") == "d").drop("__op")

        # Upsert (Merge Into)
        if not upsert_df.isEmpty():
            upsert_df.createOrReplaceGlobalTempView(f"upsert_view_{table}")
            columns = upsert_df.columns
            update_expr = ", ".join([f"t.{c} = s.{c}" for c in columns])
            insert_cols = ", ".join(columns)
            insert_vals = ", ".join([f"s.{c}" for c in columns])
            logger.info(f"Executing Merge Into for {full_table_name}")
            spark.sql(
                dedent(f"""
                MERGE INTO {full_table_name} t
                USING (SELECT * FROM global_temp.upsert_view_{table}) s
                ON t.id_iceberg = s.id_iceberg
                WHEN MATCHED THEN UPDATE SET {update_expr}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """)
            )

        # Delete
        if not delete_df.isEmpty():
            delete_df.createOrReplaceGlobalTempView(f"delete_view_{table}")
            logger.info(f"Executing Delete for {full_table_name}")
            spark.sql(
                dedent(f"""
                DELETE FROM {full_table_name} t
                WHERE EXISTS (
                    SELECT s.id_iceberg FROM global_temp.delete_view_{table} s
                    WHERE s.id_iceberg = t.id_iceberg
                )
            """)
            )

    # Watermark 기록
    table_duration = time.monotonic() - table_start_time
    stats = batch_df.agg(
        F.count("*").alias("cnt"),
        F.date_format(F.max("timestamp"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("max_ts"),
        F.min("offset").alias("min_offset"),
        F.max("offset").alias("max_offset"),
    ).collect()[0]

    batch_df.unpersist()

    append_watermark(
        spark,
        ctx.config,
        ctx.dag_id,
        iceberg_schema,
        iceberg_table,
        event_count=stats["cnt"],
        max_event_ts=stats["max_ts"],
        min_offset=stats["min_offset"],
        max_offset=stats["max_offset"],
        batch_id=batch_id,
        processing_duration_sec=table_duration,
    )


# ---------------------------------------------------------------------------
# Stream Runner
# ---------------------------------------------------------------------------


def run_topic_stream(spark: SparkSession, settings: Settings, topic: str, dag_id: str) -> None:
    logger = SparkLoggerManager().get_logger()

    if not settings.kafka:
        raise ValueError("Kafka configuration is missing.")

    checkpoint_path = f"{settings.WAREHOUSE}/checkpoints/kafka_to_iceberg/{topic}"
    logger.info(f"Starting stream for topic: {topic}, checkpoint: {checkpoint_path}")

    ctx = PipelineContext(
        spark=spark,
        config=settings,
        schema_registry_client=SchemaRegistryClient({"url": settings.kafka.schema_registry}),
        topic=topic,
        dag_id=dag_id,
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", topic)
        .option("maxOffsetsPerTrigger", settings.kafka.max_offsets_per_trigger)
        .option("startingOffsets", settings.kafka.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    query = (
        kafka_df.withColumn("key_schema_id", F.expr("byte_to_int(substring(key, 2, 4))"))
        .withColumn("key", F.expr("substring(key, 6, length(key)-5)"))
        .withColumn("value_schema_id", F.expr("byte_to_int(substring(value, 2, 4))"))
        .withColumn("value", F.expr("substring(value, 6, length(value)-5)"))
        .selectExpr("key_schema_id", "value_schema_id", "key", "value", "topic", "offset", "timestamp")
        .writeStream.foreachBatch(lambda batch_df, batch_id: process_batch(batch_df, batch_id, ctx))
        .option("checkpointLocation", checkpoint_path)
        .queryName(topic)
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "--dag-id", type=str, required=True, help="파이프라인 식별자 (watermark, signal, metrics에 사용)"
    )
    parser.add_argument("--topics", type=str, required=True)
    parser.add_argument("--concurrency", type=int, default=3, help="동시 처리 토픽 수 (기본값: 3)")
    args = parser.parse_args()
    settings = Settings()
    dag_id = args.dag_id

    topics = args.topics.split(",")
    spark = (
        SparkSession.builder.appName("kafka_to_iceberg")
        .config("spark.sql.defaultCatalog", settings.CATALOG)
        .config(f"spark.sql.catalog.{settings.CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{settings.CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{settings.CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{settings.CATALOG}.warehouse", settings.WAREHOUSE)
        .config(f"spark.sql.catalog.{settings.CATALOG}.s3.path-style-access", "true")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
        )
        .config("spark.rdd.compress", "true")
        .config("spark.sql.caseSensitive", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.shuffle.service.removeShuffle", "true")
        .config("spark.python.use.pinned.thread", "true")
        .config("spark.scheduler.mode", "FAIR")
        .getOrCreate()
    )

    # 로거 초기화
    logger_manager = SparkLoggerManager()
    logger_manager.setup(spark)
    logger = logger_manager.get_logger()

    # S3 시그널 파일 확인 (s3a://{bucket}/spark/signal/{dag_id})
    stop_signal_path = build_signal_path(settings.aws.s3_bucket, dag_id)

    # 리스너 등록 (마이크로 배치별 진행 로깅 + 시그널 감지)
    spark.streams.addListener(BatchProgressListener(signal_spark=spark, signal_path=stop_signal_path))

    # UDF 등록
    spark.udf.register("byte_to_int", lambda x: int.from_bytes(x, byteorder="big", signed=False))

    # CDC Watermark 테이블 초기화
    ensure_watermark_table(spark, settings)
    if check_stop_signal(spark, stop_signal_path):
        logger.warn(f"Stop signal detected at {stop_signal_path}. Exiting.")
        spark.stop()
        sys.exit(0)

    exceptions = []
    semaphore = threading.Semaphore(args.concurrency)
    logger.info(f"Processing {len(topics)} topics with concurrency={args.concurrency}")

    def wrapper(t_spark, t_settings, t_topic):
        with semaphore:
            try:
                if check_stop_signal(t_spark, stop_signal_path):
                    SparkLoggerManager().get_logger().warn(f"Stop signal detected. Skipping topic: {t_topic}")
                    return

                t_spark.sparkContext.setLocalProperty("spark.scheduler.pool", t_topic)
                t_spark.sparkContext.setLocalProperty("datahub.task.id", t_topic)
                t_spark.sparkContext.setJobGroup(t_topic, f"Processing {t_topic}")
                run_topic_stream(t_spark, t_settings, t_topic, dag_id)
            except Exception as e:
                SparkLoggerManager().get_logger().error(f"Failed to process topic: {t_topic}, error: {e}")
                exceptions.append(e)

    threads = []
    for topic in topics:
        t = InheritableThread(target=wrapper, args=(spark, settings, topic))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    if exceptions:
        logger.error(f"Job failed: {len(exceptions)} topic(s) had errors.")
        sys.exit(1)

    cleanup_stop_signal(spark, stop_signal_path)
    spark.stop()

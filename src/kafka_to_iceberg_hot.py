"""
Hot Tier CDC Pipeline: 토픽별 독립 readStream + availableNow() 순차 처리

기존 kafka_to_iceberg_batch.py와의 차이점:
- 토픽 1개 = readStream 1개, 한 테이블을 완전히 처리한 후 다음 테이블로 이동
- 토픽별 독립 checkpoint (checkpoints/hot/{topic_name}/)
- availableNow() 트리거 유지 (SS checkpoint 관리)
- Airflow에서 30분 간격으로 스케줄링

사용법:
  CONFIG_PATH=/path/to/settings-hot.yml spark-submit kafka_to_iceberg_hot.py
"""

import json
import os
import time

import pyspark.sql.functions as F
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro

from kafka_to_iceberg_batch import (
    cast_column,
    ensure_watermark_table,
    extract_debezium_schema,
    process_table,
    upsert_watermark,
)
from utils.settings import Settings
from utils.spark_logging import SparkLoggerManager


def process_single_table_batch(
    batch_df: DataFrame,
    batch_id: int,
    spark: SparkSession,
    config: Settings,
    schema_registry_client: SchemaRegistryClient,
    schema: str,
    table: str,
) -> None:
    """단일 테이블의 foreachBatch 핸들러. 토픽 필터링 없이 전체 배치를 처리한다."""
    logger = SparkLoggerManager().get_logger()
    iceberg_schema = f"{schema.lower()}_bronze"
    iceberg_table = table.lower()
    table_start_time = time.monotonic()

    logger.info(f"<hot-batch-{batch_id}> Processing {schema}.{table}")

    if batch_df.isEmpty():
        upsert_watermark(
            spark,
            config,
            config.kafka.metric_namespace,
            iceberg_schema,
            iceberg_table,
            event_count=0,
            max_event_ts=None,
            batch_id=batch_id,
        )
        return

    batch_df.persist(StorageLevel.MEMORY_AND_DISK)

    # 메시지 내 Schema ids 추출 및 Schema registry 조회
    value_schema_ids = [row.value_schema_id for row in batch_df.select("value_schema_id").distinct().collect()]
    value_schema_dict = {sid: schema_registry_client.get_schema(sid).schema_str for sid in value_schema_ids}

    key_schema_ids = [row.key_schema_id for row in batch_df.select("key_schema_id").distinct().collect()]
    key_schema_dict = {sid: schema_registry_client.get_schema(sid).schema_str for sid in key_schema_ids}

    logger.info(
        f"Processing {schema}.{table} | Key Schema Ids: {key_schema_ids} | Value Schema Ids: {value_schema_ids}"
    )

    # Schema ID 별로 DataFrame 필터링 후 처리
    for value_schema_id, value_schema_str in value_schema_dict.items():
        schema_filtered_df = batch_df.filter(F.col("value_schema_id") == value_schema_id)
        value_schema = json.loads(value_schema_str)
        debezium_schema = extract_debezium_schema(value_schema)

        current_key_schema_rows = schema_filtered_df.select("key_schema_id").distinct().collect()
        if not current_key_schema_rows:
            continue

        key_schema_id = current_key_schema_rows[0].key_schema_id
        key_schema_str = key_schema_dict.get(key_schema_id, "")
        if not key_schema_str:
            logger.warn(f"Key schema not found for id {key_schema_id}")
            continue

        key_schema = json.loads(key_schema_str)
        pk_cols = [field["name"] for field in key_schema["fields"]]

        # Avro Deserialization & Transformation
        transformed_df = (
            schema_filtered_df.withColumn("key", from_avro(F.col("key"), key_schema_str, {"mode": "FAILFAST"}))
            .withColumn("value", from_avro(F.col("value"), value_schema_str, {"mode": "FAILFAST"}))
            .withColumn(
                "id_iceberg",
                F.md5(
                    F.concat_ws(
                        "|",
                        *[cast_column(F.col(f"key.{column}"), debezium_schema.get(column, "")) for column in pk_cols],
                    )
                ),
            )
        )

        transformed_df = transformed_df.select(
            "value.after.*",
            F.col("value.op").alias("__op"),
            F.col("offset").alias("__offset"),
            F.timestamp_millis(F.col("value.ts_ms")).alias("last_applied_date"),
            F.col("id_iceberg"),
        )

        process_table(spark, config, schema, table, transformed_df, debezium_schema, pk_cols)

    # watermark 기록
    table_duration = time.monotonic() - table_start_time
    stats = batch_df.agg(
        F.count("*").alias("cnt"),
        F.max("timestamp").alias("max_ts"),
        F.min("offset").alias("min_offset"),
        F.max("offset").alias("max_offset"),
    ).collect()[0]
    upsert_watermark(
        spark,
        config,
        config.kafka.metric_namespace,
        iceberg_schema,
        iceberg_table,
        event_count=stats["cnt"],
        max_event_ts=stats["max_ts"],
        min_offset=stats["min_offset"],
        max_offset=stats["max_offset"],
        batch_id=batch_id,
        processing_duration_sec=table_duration,
    )

    batch_df.unpersist()
    logger.info(f"<hot-batch-{batch_id}> Completed {schema}.{table} in {table_duration:.1f}s, events={stats['cnt']}")


def main():
    settings = Settings()
    os.environ["AWS_PROFILE"] = settings.AWS_PROFILE

    spark = (
        SparkSession.builder.appName("KafkaToIcebergHot")
        .config("spark.sql.defaultCatalog", settings.CATALOG)
        .config(f"spark.sql.catalog.{settings.CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{settings.CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{settings.CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{settings.CATALOG}.warehouse", settings.ICEBERG_S3_ROOT_PATH)
        .config(f"spark.sql.catalog.{settings.CATALOG}.s3.path-style-access", "true")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
        )
        .config("spark.rdd.compress", "true")
        .config("spark.sql.caseSensitive", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.streaming.stopGracefulOnShutdown", "true")
        .config("spark.shuffle.service.removeShuffle", "true")
        .config("spark.metrics.namespace", settings.kafka.metric_namespace)
        .getOrCreate()
    )

    # 로거 초기화
    logger_manager = SparkLoggerManager()
    logger_manager.setup(spark)
    logger = logger_manager.get_logger()

    # CDC Watermark 테이블 초기화
    ensure_watermark_table(spark, settings)

    schema_registry_client = SchemaRegistryClient({"url": settings.kafka.schema_registry})

    # UDF 등록
    spark.udf.register("byte_to_int", lambda x: int.from_bytes(x, byteorder="big", signed=False))

    job_start_time = time.monotonic()
    total_tables = len(settings.TABLE_LIST)

    logger.info(f"[Hot Tier] Starting CDC pipeline for {total_tables} tables")

    # 테이블별 독립 readStream + 순차 처리
    for idx, table_identifier in enumerate(settings.TABLE_LIST, start=1):
        if settings.DB_TYPE == "mysql":
            schema, table = table_identifier.split(".")
        elif settings.DB_TYPE == "sqlserver":
            schema, _, table = table_identifier.split(".", 2)
        else:
            logger.error(f"Unsupported DB_TYPE: {settings.DB_TYPE}")
            continue

        topic = f"{settings.kafka.topic_prefix}.{schema}.{table}"
        checkpoint_path = f"{settings.ICEBERG_S3_ROOT_PATH}/checkpoints/hot/{topic}"

        logger.info(f"[{idx}/{total_tables}] Starting readStream for topic: {topic}")

        # 토픽별 독립 readStream
        kafka_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
            .option("subscribe", topic)
            .option("maxOffsetsPerTrigger", settings.kafka.max_offsets_per_trigger)
            .option("startingOffsets", settings.kafka.starting_offsets)
            .option("failOnDataLoss", "false")
            .load()
        )

        # Kafka 메시지 파싱
        parsed_df = (
            kafka_df.withColumn("key_schema_id", F.expr("byte_to_int(substring(key, 2, 4))"))
            .withColumn("key", F.expr("substring(key, 6, length(key)-5)"))
            .withColumn("value_schema_id", F.expr("byte_to_int(substring(value, 2, 4))"))
            .withColumn("value", F.expr("substring(value, 6, length(value)-5)"))
            .selectExpr("key_schema_id", "value_schema_id", "key", "value", "topic", "offset", "timestamp")
        )

        # foreachBatch로 단일 테이블 처리
        query = (
            parsed_df.writeStream.foreachBatch(
                lambda batch_df, batch_id, s=schema, t=table: process_single_table_batch(
                    batch_df, batch_id, spark, settings, schema_registry_client, s, t
                )
            )
            .option("checkpointLocation", checkpoint_path)
            .outputMode("append")
            .trigger(availableNow=True)
            .start()
        )

        # 이 토픽의 모든 데이터를 처리할 때까지 대기
        query.awaitTermination()
        logger.info(f"[{idx}/{total_tables}] Completed topic: {topic}")

    job_duration = time.monotonic() - job_start_time
    logger.info(f"[Hot Tier] Pipeline completed. Total duration: {job_duration:.1f}s")

    spark.stop()


if __name__ == "__main__":
    main()

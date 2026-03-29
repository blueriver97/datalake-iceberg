"""
Kafka → S3 Parquet Pipeline (Avro 역직렬화, foreachBatch 처리)

Airflow에서 spark-submit으로 실행:
  spark-submit --py-files utils.zip kafka_to_s3.py \
    --dag-id glue_kafka_to_s3 \
    --topics "prefix.schema.table1,prefix.schema.table2" \
    --output-path s3a://bucket/data/raw/kafka

출력 경로 구조:
  {output_path}/{topic}/year=yyyy/month=MM/day=dd/

S3 시그널 파일로 중단:
  s3a://{bucket}/spark/signal/{dag_id} 파일이 존재하면 스트리밍을 종료한다.
"""

import sys
from argparse import ArgumentParser

import pyspark.sql.functions as F
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro

from utils.listener import BatchProgressListener
from utils.settings import Settings
from utils.signal import build_signal_path, check_stop_signal, cleanup_stop_signal
from utils.spark_logging import SparkLoggerManager


def process_batch(
    batch_df: DataFrame,
    batch_id: int,
    output_path: str,
    schema_registry_client: SchemaRegistryClient,
) -> None:
    """
    Kafka에서 수신한 각 배치를 처리하는 함수.
    Schema Registry를 통해 Avro 스키마를 조회하고 Parquet으로 변환하여 S3에 저장합니다.

    출력 구조: {output_path}/{topic}/year=yyyy/month=MM/day=dd/
    """
    logger = SparkLoggerManager().get_logger()

    batch_df.persist(StorageLevel.MEMORY_AND_DISK)
    row_count = batch_df.count()

    if row_count == 0:
        logger.info(f"Batch {batch_id} is empty, skipping.")
        batch_df.unpersist()
        return

    try:
        # 고유한 Schema ID 추출 및 스키마 조회
        value_schema_ids = [row.value_schema_id for row in batch_df.select("value_schema_id").distinct().collect()]
        value_schema_dict = {sid: schema_registry_client.get_schema(sid).schema_str for sid in value_schema_ids}

        for value_schema_id, value_schema_str in value_schema_dict.items():
            df = batch_df.filter(F.col("value_schema_id") == value_schema_id)
            if df.isEmpty():
                continue

            # Avro → Struct 변환 및 파티션 컬럼 생성
            processed_df = (
                df.withColumn("value", from_avro("value", value_schema_str, {"mode": "FAILFAST"}))
                .select("value.*", "topic", "timestamp")
                .withColumn("year", F.date_format("timestamp", "yyyy"))
                .withColumn("month", F.date_format("timestamp", "MM"))
                .withColumn("day", F.date_format("timestamp", "dd"))
                .drop("timestamp")
            )

            processed_df.write.format("parquet").partitionBy("topic", "year", "month", "day").mode("append").save(
                output_path
            )

        logger.info(f"Batch {batch_id}: processed {row_count} records.")
    finally:
        batch_df.unpersist()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--dag-id", type=str, required=True, help="파이프라인 식별자 (signal에 사용)")
    parser.add_argument("--topics", type=str, required=True)
    parser.add_argument("--output-path", type=str, required=True, help="S3 출력 루트 경로 (s3a://bucket/path)")
    args = parser.parse_args()
    settings = Settings()
    dag_id = args.dag_id
    output_path = args.output_path

    topics = args.topics.split(",")

    spark = (
        SparkSession.builder.appName("kafka_to_s3")
        .config("spark.sql.defaultCatalog", settings.CATALOG)
        .config(f"spark.sql.catalog.{settings.CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{settings.CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{settings.CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{settings.CATALOG}.warehouse", settings.WAREHOUSE)
        .config(f"spark.sql.catalog.{settings.CATALOG}.s3.path-style-access", "true")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
        )
        .config("spark.sql.caseSensitive", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    # 로거 초기화
    logger_manager = SparkLoggerManager()
    logger_manager.setup(spark)
    logger = logger_manager.get_logger()

    # S3 시그널 파일 확인
    stop_signal_path = build_signal_path(settings.aws.s3_bucket, dag_id)

    # 리스너 등록 (시그널 파일 감지 시 스트리밍 쿼리 종료)
    spark.streams.addListener(BatchProgressListener(signal_spark=spark, signal_path=stop_signal_path))

    # UDF 등록 (Kafka Value에서 Schema ID 추출용)
    spark.udf.register("byte_to_int", lambda x: int.from_bytes(x, byteorder="big", signed=False))

    # 시작 전 시그널 확인
    if check_stop_signal(spark, stop_signal_path):
        logger.warn(f"Stop signal detected at {stop_signal_path}. Exiting.")
        spark.stop()
        sys.exit(0)

    # Kafka 설정 검증
    if settings.kafka is None:
        raise ValueError("Kafka settings are required for kafka_to_s3.")
    kafka_config = settings.kafka

    # Schema Registry 클라이언트 생성
    schema_registry_client = SchemaRegistryClient({"url": kafka_config.schema_registry})

    logger.info(f"Subscribing to Kafka topics: {topics}")

    # Kafka 스트림 소스 정의
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_config.bootstrap_servers)
        .option("subscribe", ",".join(topics))
        .option("maxOffsetsPerTrigger", kafka_config.max_offsets_per_trigger)
        .option("startingOffsets", kafka_config.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    # Magic Byte 및 Schema ID 제거
    # Value 구조: [Magic Byte(1)] + [Schema ID(4)] + [Data]
    transformed_df = (
        kafka_df.withColumn("value_schema_id", F.expr("byte_to_int(substring(value, 2, 4))"))
        .withColumn("value", F.expr("substring(value, 6, length(value)-5)"))
        .select("value_schema_id", "value", "topic", "timestamp")
    )

    # 스트리밍 쿼리 실행
    checkpoint_path = f"{settings.WAREHOUSE}/checkpoints/kafka_to_s3"
    query = (
        transformed_df.writeStream.foreachBatch(
            lambda df, batch_id: process_batch(df, batch_id, output_path, schema_registry_client)
        )
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(processingTime="1 minutes")
        .queryName(dag_id)
        .start()
    )

    logger.info(f"Streaming query started. Checkpoint: {checkpoint_path}")
    query.awaitTermination()
    cleanup_stop_signal(spark, stop_signal_path)
    spark.stop()

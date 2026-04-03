"""
Kafka → S3 Parquet Pipeline (Avro 역직렬화, 토픽별 독립 스트림)

Airflow에서 spark-submit으로 실행:
  spark-submit --py-files utils.zip kafka_to_s3.py \
    --dag-id glue_kafka_to_s3 \
    --topics "prefix.schema.table1,prefix.schema.table2" \
    --output-path s3a://bucket/data/raw/kafka

출력 경로 구조:
  {output_path}/{topic}/year=yyyy/month=MM/day=dd/

토픽별 파티셔닝 규칙:
  --partition-by "year,month,day"  (기본값)
  --partition-rules '{"prefix.schema.events": "year,month,day,hour"}'

S3 시그널 파일로 중단:
  s3a://{bucket}/spark/signal/{dag_id} 파일이 존재하면 스트리밍을 종료한다.
"""

import json
import sys
from argparse import ArgumentParser

import pyspark.sql.functions as F
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.streaming import StreamingQuery

from utils.settings import Settings
from utils.signal import BatchProgressListener, build_signal_path, check_stop_signal, cleanup_stop_signal
from utils.spark import SparkLoggerManager, create_spark_session

# ---------------------------------------------------------------------------
# Partition
# ---------------------------------------------------------------------------

PARTITION_FORMAT = {
    "year": "yyyy",
    "month": "MM",
    "day": "dd",
    "hour": "HH",
}


# ---------------------------------------------------------------------------
# Batch Processing
# ---------------------------------------------------------------------------


def process_batch(
    batch_df: DataFrame,
    batch_id: int,
    output_path: str,
    partition_cols: list[str],
    schema_registry_client: SchemaRegistryClient,
) -> None:
    """
    Kafka에서 수신한 각 배치를 처리하는 함수.
    Schema Registry를 통해 Avro 스키마를 조회하고 Parquet으로 변환하여 S3에 저장한다.

    출력 구조: {output_path}/year=yyyy/month=MM/day=dd/
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
            processed_df = df.withColumn("value", from_avro("value", value_schema_str, {"mode": "FAILFAST"})).select(
                "value.*", "timestamp"
            )
            for col_name in partition_cols:
                fmt = PARTITION_FORMAT.get(col_name)
                if fmt:
                    processed_df = processed_df.withColumn(col_name, F.date_format("timestamp", fmt))
            processed_df = processed_df.drop("timestamp")

            processed_df.write.format("parquet").partitionBy(*partition_cols).mode("append").save(output_path)

        logger.info(f"Batch {batch_id}: processed {row_count} records.")
    finally:
        batch_df.unpersist()


# ---------------------------------------------------------------------------
# Stream Runner
# ---------------------------------------------------------------------------


def run_topic_stream(
    spark: SparkSession,
    settings: Settings,
    topic: str,
    dag_id: str,
    output_path: str,
    partition_cols: list[str],
    schema_registry_client: SchemaRegistryClient,
) -> StreamingQuery:
    logger = SparkLoggerManager().get_logger()

    if not settings.kafka:
        raise ValueError("Kafka configuration is missing.")

    checkpoint_path = f"s3a://{settings.storage.bucket}/kafka/checkpoint/{dag_id}/{topic}"
    topic_output_path = f"{output_path}/{topic}"
    logger.info(f"Starting stream for topic: {topic}, checkpoint: {checkpoint_path}")

    def _foreach_batch(batch_df: DataFrame, batch_id: int) -> None:
        """클로저: 외부 스코프의 topic, partition_cols를 캡처하여 배치 처리에 전달한다."""
        # FAIR 스케줄러에서 토픽별 독립 풀을 할당하여 스레드 간 리소스 경합을 방지한다.
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", topic)
        process_batch(batch_df, batch_id, topic_output_path, partition_cols, schema_registry_client)

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", topic)
        .option("maxOffsetsPerTrigger", settings.kafka.max_offsets_per_trigger)
        .option("startingOffsets", settings.kafka.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    # Magic Byte 및 Schema ID 제거
    # Value 구조: [Magic Byte(1)] + [Schema ID(4)] + [Data]
    transformed_df = (
        kafka_df.withColumn("value_schema_id", F.expr("byte_to_int(substring(value, 2, 4))"))
        .withColumn("value", F.expr("substring(value, 6, length(value)-5)"))
        .select("value_schema_id", "value", "timestamp")
    )

    query = (
        transformed_df.writeStream.foreachBatch(_foreach_batch)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(processingTime="1 minutes")
        .queryName(topic)
        .start()
    )
    return query


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--dag-id", type=str, required=True, help="파이프라인 식별자 (signal에 사용)")
    parser.add_argument("--topics", type=str, required=True)
    parser.add_argument("--output-path", type=str, required=True, help="S3 출력 루트 경로 (s3a://bucket/path)")
    parser.add_argument(
        "--partition-by", type=str, default="year,month,day", help="기본 파티션 컬럼 (기본값: year,month,day)"
    )
    parser.add_argument(
        "--partition-rules",
        type=str,
        default=None,
        help='토픽별 파티션 오버라이드 JSON (예: \'{"topic": "year,month,day,hour"}\')',
    )
    parser.add_argument("--env-file", type=str, default=".env", help="환경 설정 파일 경로 (기본값: .env)")
    args = parser.parse_args()
    settings = Settings(_env_file=args.env_file)
    dag_id = args.dag_id
    output_path = args.output_path

    topics = args.topics.split(",")
    default_partition = [c.strip() for c in args.partition_by.split(",")]
    partition_rules: dict[str, str] = json.loads(args.partition_rules) if args.partition_rules else {}

    spark = create_spark_session(
        "kafka_to_s3",
        settings,
        extra_configs={
            "spark.sql.caseSensitive": "true",
            "spark.scheduler.mode": "FAIR",
        },
    )

    # 로거 초기화
    logger_manager = SparkLoggerManager()
    logger_manager.setup(spark)
    logger = logger_manager.get_logger()

    # S3 시그널 파일 확인
    stop_signal_path = build_signal_path(settings.storage.bucket, dag_id)

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

    # Schema Registry 클라이언트 생성
    schema_registry_client = SchemaRegistryClient({"url": settings.kafka.schema_registry})

    logger.info(f"Starting {len(topics)} topic streams: {topics}")

    # 토픽별 독립 스트림 시작
    queries = []
    for topic in topics:
        partition_cols = (
            [c.strip() for c in partition_rules[topic].split(",")] if topic in partition_rules else default_partition
        )
        q = run_topic_stream(spark, settings, topic, dag_id, output_path, partition_cols, schema_registry_client)
        queries.append(q)

    spark.streams.awaitAnyTermination()
    cleanup_stop_signal(spark, stop_signal_path)
    spark.stop()

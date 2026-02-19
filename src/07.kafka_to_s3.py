import os

import pyspark.sql.functions as F
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro

# --- Import common modules ---
from utils.settings import Settings
from utils.spark_logging import SparkLoggerManager


def process_batch(
    batch_df: DataFrame,
    batch_id: int,
    settings: Settings,
    schema_registry_client: SchemaRegistryClient,
) -> None:
    """
    Kafka에서 수신한 각 배치를 처리하는 함수.
    Schema Registry를 통해 Avro 스키마를 조회하고 Parquet으로 변환하여 S3에 저장합니다.
    """
    logger = SparkLoggerManager().get_logger(f"Batch-{batch_id}")

    if batch_df.isEmpty():
        logger.info("Batch is empty, skipping.")
        return

    # 고유한 Schema ID 추출 및 스키마 조회
    # Note: 스키마 ID가 많을 경우 성능 이슈가 발생할 수 있으므로 캐싱 고려 가능
    value_schema_ids = [row.value_schema_id for row in batch_df.select("value_schema_id").distinct().collect()]
    value_schema_dict = {sid: schema_registry_client.get_schema(sid).schema_str for sid in value_schema_ids}

    for value_schema_id, value_schema_str in value_schema_dict.items():
        # 스키마 ID별로 데이터 필터링
        df = batch_df.filter(F.col("value_schema_id") == value_schema_id)
        if df.isEmpty():
            continue

        # Avro -> Struct 변환 및 파티션 컬럼 생성
        # from_avro 함수는 JSON 형태의 스키마 문자열을 인자로 받음
        processed_df = (
            df.withColumn("value", from_avro("value", value_schema_str, {"mode": "FAILFAST"}))
            .select("value.*", "topic", "timestamp")  # timestamp는 Kafka 메시지 타임스탬프
            .withColumn("year", F.date_format("timestamp", "yyyy"))
            .withColumn("month", F.date_format("timestamp", "MM"))
            .withColumn("day", F.date_format("timestamp", "dd"))
            .drop("timestamp")
        )

        # S3에 Parquet 포맷으로 저장
        # 파티셔닝: topic/year/month/day
        s3_path = f"{settings.ICEBERG_S3_ROOT_PATH}/sink"
        (processed_df.write.format("parquet").partitionBy("topic", "year", "month", "day").mode("append").save(s3_path))

    logger.info(f"Successfully processed {batch_df.count()} records.")


if __name__ == "__main__":
    settings = Settings()
    os.environ["AWS_PROFILE"] = settings.AWS_PROFILE

    spark = (
        SparkSession.builder.appName("KafkaToS3")
        .config("spark.sql.defaultCatalog", settings.CATALOG)
        .config(f"spark.sql.catalog.{settings.CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{settings.CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{settings.CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{settings.CATALOG}.warehouse", settings.ICEBERG_S3_ROOT_PATH)
        .config(f"spark.sql.catalog.{settings.CATALOG}.s3.path-style-access", "true")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
        )
        .config("spark.sql.caseSensitive", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.metrics.namespace", settings.kafka.metric_namespace)
        .getOrCreate()
    )

    logger_manager = SparkLoggerManager()
    logger_manager.setup(spark)
    logger = logger_manager.get_logger()

    logger.info("Spark App Name: KafkaToS3")
    # 구독할 토픽 목록 생성 (prefix + table)
    topics = [f"{settings.kafka.topic_prefix}.{t}" for t in settings.TABLE_LIST]
    logger.info(f"Subscribing to Kafka topics: {topics}")

    # Schema Registry 클라이언트 생성
    schema_registry_client = SchemaRegistryClient({"url": settings.kafka.schema_registry})

    # UDF 등록 (byte -> int)
    spark.udf.register("byte_to_int", lambda x: int.from_bytes(x, byteorder="big", signed=False))

    # Kafka 스트림 소스 정의
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", ",".join(topics))
        .option("maxOffsetsPerTrigger", settings.kafka.max_offsets_per_trigger)
        .option("startingOffsets", settings.kafka.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    # 스트리밍 쿼리 전처리: Magic Byte 및 Schema ID 제거
    # Value 구조: [Magic Byte(1)] + [Schema ID(4)] + [Data]
    transformed_df = (
        kafka_df.withColumn("value_schema_id", F.expr("byte_to_int(substring(value, 2, 4))"))
        .withColumn("value", F.expr("substring(value, 6, length(value)-5)"))
        .select("value_schema_id", "value", "topic", "timestamp")
    )

    # 스트리밍 쿼리 실행
    query = (
        transformed_df.writeStream.foreachBatch(
            lambda df, batch_id: process_batch(df, batch_id, settings, schema_registry_client)
        )
        .option("checkpointLocation", f"{settings.ICEBERG_S3_ROOT_PATH}/checkpoints/kafka_to_s3")
        .outputMode("append")
        .trigger(processingTime="1 minutes")
        .start()
    )

    query.awaitTermination()
    spark.stop()

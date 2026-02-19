import json
import os
from textwrap import dedent

import pyspark.sql.functions as F
import pyspark.sql.types as T
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql.avro.functions import from_avro

# --- Import common modules ---
from utils.settings import Settings
from utils.spark_logging import SparkLoggerManager


def extract_debezium_schema(schema: dict) -> dict:
    """
    Debezium 메시지 스키마에서 컬럼 이름과 Debezium 커넥터 타입을 추출합니다.

    Args:
        schema: 파싱할 Debezium JSON 스키마 딕셔너리.

    Returns:
        {column_name: dbz_connector_type} 형태의 딕셔너리.
    """
    debezium_type = {}

    # 1. Envelope 스키마의 'fields' 목록에서 'before' 또는 'after' 필드를 찾습니다.
    envelope_fields = schema.get("fields", [])
    value_schema = None
    for field in envelope_fields:
        if field.get("name") in ("before", "after"):
            # 타입 정보는 ['null', {실제 스키마}] 형태일 수 있습니다.
            type_definitions = field.get("type", [])
            for type_def in type_definitions:
                if isinstance(type_def, dict) and "fields" in type_def:
                    value_schema = type_def
                    break
            if value_schema:
                break

    if not value_schema:
        print("Error: 'before' 또는 'after' 필드에서 유효한 스키마를 찾을 수 없습니다.")
        return {}

    # 2. 찾은 스키마 내부의 'fields' (컬럼 목록)를 순회합니다.
    column_fields = value_schema.get("fields", [])
    for col_field in column_fields:
        col_name = col_field.get("name")
        if not col_name:
            continue

        type_info = col_field.get("type")

        # 3. 컬럼 타입을 처리합니다. (Nullable 여부 고려)
        actual_type_def = None
        if isinstance(type_info, list):
            # Nullable 타입: ['null', type] 형태에서 실제 타입 정보를 찾습니다.
            for item in type_info:
                if item != "null":
                    actual_type_def = item
                    break
        else:
            # Non-nullable 타입
            actual_type_def = type_info

        if not actual_type_def:
            continue

        # 4. 최종 커넥터 타입을 추출합니다.
        # 복합 타입(dict)인 경우 'connect.name'을 우선적으로 사용하고,
        # 없는 경우 'type'을 사용합니다.
        # 단순 타입(str)인 경우 해당 문자열을 그대로 사용합니다.
        dbz_connector_type = None
        if isinstance(actual_type_def, dict):
            dbz_connector_type = actual_type_def.get("connect.name")
            if not dbz_connector_type:
                dbz_connector_type = actual_type_def.get("type")
        elif isinstance(actual_type_def, str):
            dbz_connector_type = actual_type_def

        if dbz_connector_type:
            debezium_type[col_name] = dbz_connector_type

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
    """
    if debezium_dtype == "io.debezium.time.Date":
        return F.date_add(F.lit("1970-01-01"), column.cast("int"))
    elif debezium_dtype == "io.debezium.time.MicroTime":
        return F.to_utc_timestamp(F.timestamp_seconds(column / 1_000_000), "UTC")
    elif debezium_dtype == "io.debezium.time.Timestamp":
        return F.to_utc_timestamp(F.timestamp_millis(column), "Asia/Seoul")
    elif debezium_dtype == "io.debezium.time.MicroTimestamp":
        return F.to_utc_timestamp(F.timestamp_micros(column), "Asia/Seoul")
    elif debezium_dtype == "io.debezium.time.ZonedTimestamp":
        pass
    return column


def cast_dataframe(df: DataFrame, catalog_schema: T.StructType, debezium_schema: dict) -> DataFrame:
    """
    DataFrame의 컬럼들을 Iceberg 테이블 스키마에 맞게 변환합니다.

    Args:
        df: 변환할 DataFrame
        catalog_schema: Iceberg 테이블의 스키마 정보
        debezium_schema: Debezium 커넥터의 스키마 정보

    Returns:
        Iceberg 테이블 스키마로 변환된 DataFrame
    """
    return df.select(
        *[
            cast_column(F.col(field.name), debezium_schema.get(field.name, "")).cast(field.dataType).alias(field.name)
            for field in catalog_schema.fields
        ],
        "__op",  # 작업 유형 (insert/update/delete)
        "__offset",  # Kafka 오프셋 값
    )


def process_table(
    spark: SparkSession,
    config: Settings,
    schema: str,
    table: str,
    table_df: DataFrame,
    debezium_schema: dict,
    pk_cols: list,
) -> None:
    logger = SparkLoggerManager().get_logger(__name__)

    iceberg_schema, iceberg_table = f"{schema.lower()}_bronze", table.lower()
    full_table_name = f"{config.CATALOG}.{iceberg_schema}.{iceberg_table}"

    # Iceberg 테이블 스키마 조회
    try:
        catalog_schema = spark.table(full_table_name).schema
    except Exception:
        logger.warn(f"Table {full_table_name} not found. Skipping.")
        return

    cdc_df = cast_dataframe(table_df, catalog_schema, debezium_schema)

    # 중복 제거 (동일 키에 대해 오프셋이 가장 큰 최신 레코드만 유지)
    window_spec = Window.partitionBy("id_iceberg").orderBy(F.desc("__offset"))
    cdc_df = (
        cdc_df.withColumn("__row", F.row_number().over(window_spec))
        .filter(F.col("__row") == 1)
        .drop("__row", "__offset")
    )
    upsert_df = cdc_df.filter(F.col("__op") != "d").drop("__op")
    delete_df = cdc_df.filter(F.col("__op") == "d").drop("__op")

    # Upsert 처리 (Merge Into)
    if not upsert_df.isEmpty():
        upsert_df.createOrReplaceGlobalTempView("upsert_view")
        columns = spark.table(full_table_name).columns
        update_expr = ", ".join([f"t.{c} = s.{c}" for c in columns])
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join([f"s.{c}" for c in columns])

        query = dedent(f"""
            MERGE INTO {full_table_name} t
            USING (SELECT * FROM global_temp.upsert_view) s
            ON t.id_iceberg = s.id_iceberg
            WHEN MATCHED THEN UPDATE SET {update_expr}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """)
        logger.info(f"Executing Merge Into for {full_table_name}")
        spark.sql(query)

    # Delete 처리
    if not delete_df.isEmpty():
        delete_df.createOrReplaceGlobalTempView("delete_view")
        query = dedent(f"""
            DELETE FROM {full_table_name} t
            WHERE EXISTS (
                SELECT s.id_iceberg
                FROM global_temp.delete_view s
                WHERE s.id_iceberg = t.id_iceberg
            )
        """)
        logger.info(f"Executing Delete for {full_table_name}")
        spark.sql(query)


def process_batch(
    batch_df: DataFrame,
    batch_id: int,
    spark: SparkSession,
    config: Settings,
    schema_registry_client: SchemaRegistryClient,
) -> None:
    logger = SparkLoggerManager().get_logger(__name__)

    logger.info(f"<batch-{batch_id}, {batch_df.count()}>")
    if batch_df.isEmpty():
        return

    batch_df.persist(StorageLevel.MEMORY_AND_DISK)

    # 설정된 테이블 목록을 순회하며 처리
    for table_identifier in config.TABLE_LIST:
        if config.DB_TYPE == "mysql":
            schema, table = table_identifier.split(".")
        elif config.DB_TYPE == "mssql":
            schema, _, table = table_identifier.split(".", 2)
        else:
            logger.error(f"Unsupported DB_TYPE: {config.DB_TYPE}")
            continue

        # 해당 테이블의 토픽 이름 조회 (Settings 클래스에 TOPIC_DICT 구현 필요 가정)
        # 현재 Settings에는 TOPIC_DICT가 없으므로, topic_prefix 등을 이용해 추론하거나
        # Settings 클래스에 해당 프로퍼티를 추가해야 함.
        # 여기서는 기존 코드 로직을 따르되, Settings에 TOPIC_DICT가 있다고 가정하거나
        # topic_prefix를 사용하여 토픽명을 구성함.
        # 예: local.schema.table
        target_topic = f"{config.kafka.topic_prefix}.{schema}.{table}"

        filtered_df = batch_df.filter(F.col("topic") == target_topic)

        if filtered_df.isEmpty():
            continue

        # 메시지 내 Schema ids 추출 및 Schema registry 조회
        value_schema_ids = [row.value_schema_id for row in filtered_df.select("value_schema_id").distinct().collect()]
        value_schema_dict = {sid: schema_registry_client.get_schema(sid).schema_str for sid in value_schema_ids}

        key_schema_ids = [row.key_schema_id for row in filtered_df.select("key_schema_id").distinct().collect()]
        key_schema_dict = {sid: schema_registry_client.get_schema(sid).schema_str for sid in key_schema_ids}

        logger.info(
            f"Processing {table_identifier} | Key Schema Ids: {key_schema_ids} | Value Schema Ids: {value_schema_ids}"
        )

        # Schema ID 별로 DataFrame 필터링 후 처리
        for value_schema_id, value_schema_str in value_schema_dict.items():
            schema_filtered_df = filtered_df.filter(F.col("value_schema_id") == value_schema_id)
            value_schema = json.loads(value_schema_str)
            debezium_schema = extract_debezium_schema(value_schema)

            # Key Schema 처리
            # 해당 배치의 해당 value_schema_id에 매핑되는 key_schema_id를 찾음
            # (단순화를 위해 첫 번째 key_schema_id 사용 - 일반적으로 테이블당 하나의 키 스키마)
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
                            *[
                                cast_column(F.col(f"key.{column}"), debezium_schema.get(column, ""))
                                for column in pk_cols
                            ],
                        )
                    ),
                )
            )

            # 필요한 컬럼 선택 및 이름 변경
            # value.after.* : 변경 후 데이터
            # value.op : 작업 유형
            # value.ts_ms : 소스 DB 변경 시간
            # offset : Kafka 오프셋
            # id_iceberg : Iceberg PK (Hash)

            # 스키마에 없는 컬럼 접근 시 에러 방지를 위해 * 사용 보다는 명시적 컬럼 선택이 좋으나,
            # 동적 스키마 처리를 위해 value.after.* 사용.
            # 단, value.after가 null인 경우 (delete) 고려 필요.
            # Delete 시에는 value.before를 사용해야 할 수도 있으나,
            # Debezium 설정에 따라 delete 시 after가 null일 수 있음.
            # 여기서는 기존 로직(value.after.*)을 따르되, delete 시에는 id_iceberg만 있으면 됨.

            # 개선: op가 'd'인 경우 value.before를 참조하거나, id_iceberg만으로 삭제 처리.
            # 현재 로직은 value.after.*를 select 하므로 delete 시 null로 채워질 수 있음.

            transformed_df = transformed_df.select(
                "value.after.*",
                F.col("value.op").alias("__op"),
                F.col("offset").alias("__offset"),
                F.col("value.ts_ms").alias("last_applied_date"),
                F.col("id_iceberg"),
            )

            process_table(spark, config, schema, table, transformed_df, debezium_schema, pk_cols)

    batch_df.unpersist()


if __name__ == "__main__":
    settings = Settings()
    os.environ["AWS_PROFILE"] = settings.AWS_PROFILE

    spark = (
        SparkSession.builder.appName("KafkaToIcebergBatch")
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

    schema_registry_client = SchemaRegistryClient({"url": settings.kafka.schema_registry})

    # UDF 등록: Byte Array -> Integer (Schema ID 추출용)
    spark.udf.register("byte_to_int", lambda x: int.from_bytes(x, byteorder="big", signed=False))

    # Kafka ReadStream 설정
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", ",".join([f"{settings.kafka.topic_prefix}.{t}" for t in settings.TABLE_LIST]))
        .option("maxOffsetsPerTrigger", settings.kafka.max_offsets_per_trigger)
        .option("startingOffsets", settings.kafka.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    # Kafka 메시지 파싱 및 배치 처리
    # Key/Value 구조: [Magic Byte(1)] + [Schema ID(4)] + [Data]
    query = (
        kafka_df.withColumn("key_schema_id", F.expr("byte_to_int(substring(key, 2, 4))"))
        .withColumn("key", F.expr("substring(key, 6, length(key)-5)"))
        .withColumn("value_schema_id", F.expr("byte_to_int(substring(value, 2, 4))"))
        .withColumn("value", F.expr("substring(value, 6, length(value)-5)"))
        .selectExpr("key_schema_id", "value_schema_id", "key", "value", "topic", "offset")
        .writeStream.foreachBatch(
            lambda batch_df, batch_id: process_batch(batch_df, batch_id, spark, settings, schema_registry_client)
        )
        .option("checkpointLocation", f"{settings.ICEBERG_S3_ROOT_PATH}/checkpoints/kafka_to_iceberg")
        .outputMode("append")
        .trigger(availableNow=True)
        # .trigger(processingTime="5 minutes")
        .start()
    )

    query.awaitTermination()
    spark.stop()

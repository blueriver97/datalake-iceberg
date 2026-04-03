"""
Kafka CDC → Iceberg Streaming Pipeline (상시 실행, 라운드 기반 반복 처리)

kafka_to_iceberg.py의 batch 모드를 while 루프로 감싸 상시 실행한다.
각 라운드에서 availableNow=True 트리거로 가용 메시지를 처리한 뒤,
round-interval 간격으로 다음 라운드를 시작한다.

Airflow에서 spark-submit으로 실행:
  spark-submit --py-files utils.zip kafka_to_iceberg_stream.py \
    --dag-id "my_stream_job" --topics "prefix.schema.table1,..." \
    --concurrency 3 --round-interval 300

S3 시그널 파일로 중단:
  s3a://{bucket}/spark/signal/{dag_id} 파일이 존재하면 현재 라운드 완료 후 종료.
"""

import base64
import json
import sys
import threading
import time
from argparse import ArgumentParser

from pyspark import InheritableThread
from pyspark.sql import SparkSession

from utils.cdc_pipeline import run_topic_stream
from utils.maintenance import ProcessedTableTracker, run_compaction
from utils.settings import Settings
from utils.signal import BatchProgressListener, build_signal_path, check_stop_signal, cleanup_stop_signal
from utils.spark import SparkLoggerManager, create_spark_session
from utils.watermark import ensure_watermark_tables, get_last_completed_map, should_run

# ---------------------------------------------------------------------------
# Round Runner
# ---------------------------------------------------------------------------


def _run_one_round(
    spark: SparkSession,
    settings: Settings,
    topics: list[str],
    dag_id: str,
    concurrency: int,
    stop_signal_path: str,
    offsets_map: dict,
    scheduled_at: str | None = None,
    tracker: ProcessedTableTracker | None = None,
    position_delete_interval: int = 0,
    position_delete_last_map: dict | None = None,
) -> list[Exception]:
    """한 라운드 실행: N개 토픽을 세마포어 기반 concurrency로 병렬 처리.

    Returns:
        라운드 내 발생한 예외 목록 (빈 리스트 = 모두 성공).
    """
    semaphore = threading.Semaphore(concurrency)
    round_exceptions: list[Exception] = []

    def wrapper(t_spark: SparkSession, t_settings: Settings, t_topic: str) -> None:
        with semaphore:
            try:
                if check_stop_signal(t_spark, stop_signal_path):
                    SparkLoggerManager().get_logger().warn(f"Stop signal detected. Skipping topic: {t_topic}")
                    return

                # FAIR 스케줄러에서 토픽별 독립 풀을 할당하여 스레드 간 리소스 경합을 방지한다.
                t_spark.sparkContext.setLocalProperty("spark.scheduler.pool", t_topic)
                t_spark.sparkContext.setLocalProperty("datahub.task.id", t_topic)
                t_spark.sparkContext.setJobGroup(t_topic, f"Processing {t_topic}")
                run_topic_stream(
                    t_spark,
                    t_settings,
                    t_topic,
                    dag_id,
                    offsets_map.get(t_topic),
                    scheduled_at,
                    tracker=tracker,
                    position_delete_interval=position_delete_interval,
                    position_delete_last_map=position_delete_last_map,
                )
            except Exception as e:
                SparkLoggerManager().get_logger().error(f"Failed to process topic: {t_topic}, error: {e}")
                round_exceptions.append(e)

    threads = []
    for topic in topics:
        t = InheritableThread(target=wrapper, args=(spark, settings, topic))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    return round_exceptions


def _interruptible_sleep(spark: SparkSession, stop_signal_path: str, seconds: float) -> bool:
    """signal을 체크하며 대기. signal 감지 시 True 반환."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if check_stop_signal(spark, stop_signal_path):
            return True
        time.sleep(min(10, max(0, deadline - time.monotonic())))
    return False


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
    parser.add_argument("--starting-offsets-map", type=str, default=None, help="토픽별 시작 offset JSON (v1 전환용)")
    parser.add_argument("--round-interval", type=int, default=300, help="라운드 간 목표 간격 초 (기본값: 300)")
    parser.add_argument("--scheduled-at", type=str, default=None, help="Airflow logical_date (배치 식별용)")
    parser.add_argument(
        "--compaction-interval",
        type=int,
        default=14400,
        help="rewrite_data_files + expire_snapshots 실행 간격 초 (기본값: 14400 = 4시간)",
    )
    parser.add_argument(
        "--position-delete-interval",
        type=int,
        default=0,
        help="rewrite_position_delete_files 실행 간격 초 (기본값: 0 = 비활성, MoR 전환 시 3600)",
    )
    parser.add_argument("--env-file", type=str, default=".env", help="환경 설정 파일 경로 (기본값: .env)")
    args = parser.parse_args()

    settings = Settings(_env_file=args.env_file)
    dag_id = args.dag_id

    # starting-offsets-map 디코딩 예시:
    #   args.starting_offsets_map (base64): "eyJwcmVmaXguc2NoZW1hLnRhYmxlMSI6IHsiMCI6IDEwMH19"
    #   base64.b64decode(...)    (bytes):  b'{"prefix.schema.table1": {"0": 100}}'
    #   .decode()                (str):    '{"prefix.schema.table1": {"0": 100}}'
    #   json.loads(...)          (dict):   {"prefix.schema.table1": {"0": 100}}
    if args.starting_offsets_map:
        offsets_map = json.loads(base64.b64decode(args.starting_offsets_map).decode())
    else:
        offsets_map = {}
    topics = args.topics.split(",")

    spark = create_spark_session(
        "kafka_to_iceberg_stream",
        settings,
        extra_configs={
            "spark.rdd.compress": "true",
            "spark.sql.caseSensitive": "true",
            "spark.shuffle.service.removeShuffle": "true",
            "spark.python.use.pinned.thread": "true",
            "spark.scheduler.mode": "FAIR",
        },
    )

    # 로거 초기화
    logger_manager = SparkLoggerManager()
    logger_manager.setup(spark)
    logger = logger_manager.get_logger()

    # S3 시그널 파일 확인 (s3a://{bucket}/spark/signal/{dag_id})
    stop_signal_path = build_signal_path(settings.storage.bucket, dag_id)

    # 리스너 등록 (마이크로 배치별 진행 로깅 + 시그널 감지)
    spark.streams.addListener(BatchProgressListener(signal_spark=spark, signal_path=stop_signal_path))

    # UDF 등록
    spark.udf.register("byte_to_int", lambda x: int.from_bytes(x, byteorder="big", signed=False))

    # Watermark 테이블 초기화 (cdc_watermark + maintenance_watermark)
    ensure_watermark_tables(spark, settings.CATALOG, settings.WAREHOUSE)

    if check_stop_signal(spark, stop_signal_path):
        logger.warn(f"Stop signal detected at {stop_signal_path}. Exiting.")
        spark.stop()
        sys.exit(0)

    # 토픽 → Iceberg 테이블 키 매핑 (벌크 쿼리용)
    table_keys = []
    for topic in topics:
        _prefix, schema, table = topic.split(".")
        table_keys.append(f"{schema.lower()}_bronze.{table.lower()}")

    tracker = ProcessedTableTracker()

    logger.info(
        f"Starting streaming loop: {len(topics)} topics, "
        f"concurrency={args.concurrency}, round_interval={args.round_interval}s"
    )

    round_number = 0
    while not check_stop_signal(spark, stop_signal_path):
        round_number += 1
        round_start = time.monotonic()
        logger.info(f"=== Round {round_number}: processing {len(topics)} topics ===")

        # 라운드 시작 전: position delete compaction용 pre-fetched map
        pdc_last_map = None
        if args.position_delete_interval > 0:
            pdc_last_map = get_last_completed_map(
                spark,
                settings.CATALOG,
                table_keys,
                "rewrite_position_delete_files",
            )

        exceptions = _run_one_round(
            spark=spark,
            settings=settings,
            topics=topics,
            dag_id=dag_id,
            concurrency=args.concurrency,
            stop_signal_path=stop_signal_path,
            offsets_map=offsets_map,
            scheduled_at=args.scheduled_at,
            tracker=tracker,
            position_delete_interval=args.position_delete_interval,
            position_delete_last_map=pdc_last_map,
        )

        if exceptions:
            logger.error(f"Round {round_number} had {len(exceptions)} error(s). Exiting.")
            cleanup_stop_signal(spark, stop_signal_path)
            spark.stop()
            sys.exit(1)

        # --- Compaction phase (라운드 내 모든 스레드 완료 후) ---
        modified_tables = tracker.get_and_clear()
        if modified_tables:
            modified_keys = []
            for full_table_name in modified_tables:
                _catalog, bronze_schema, table_name = full_table_name.split(".")
                modified_keys.append(f"{bronze_schema}.{table_name}")

            compaction_map = get_last_completed_map(
                spark,
                settings.CATALOG,
                modified_keys,
                "rewrite_data_files",
            )
            for full_table_name in modified_tables:
                _catalog, bronze_schema, table_name = full_table_name.split(".")
                key = f"{bronze_schema}.{table_name}"
                if should_run(compaction_map.get(key), args.compaction_interval):
                    run_compaction(spark, settings.CATALOG, dag_id, full_table_name)

        round_elapsed = time.monotonic() - round_start
        sleep_seconds = max(0, args.round_interval - round_elapsed)
        if sleep_seconds > 0 and not check_stop_signal(spark, stop_signal_path):
            logger.info(
                f"Round {round_number} took {round_elapsed:.0f}s. Sleeping {sleep_seconds:.0f}s before next round."
            )
            if _interruptible_sleep(spark, stop_signal_path, sleep_seconds):
                logger.warn("Stop signal detected during sleep. Exiting.")
                break
        else:
            logger.info(
                f"Round {round_number} took {round_elapsed:.0f}s "
                f"(exceeds interval {args.round_interval}s). Starting next round immediately."
            )

        # starting-offsets-map은 첫 라운드에서만 의미 (이후 체크포인트에서 복원)
        offsets_map = {}

    logger.info(f"Streaming loop ended after {round_number} round(s).")
    cleanup_stop_signal(spark, stop_signal_path)
    spark.stop()

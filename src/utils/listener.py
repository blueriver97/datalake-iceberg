from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.streaming.listener import (
    QueryIdleEvent,
    QueryProgressEvent,
    QueryStartedEvent,
    QueryTerminatedEvent,
    StreamingQueryListener,
)

from utils.spark_logging import SparkLoggerManager


class BatchProgressListener(StreamingQueryListener):
    """마이크로 배치 진행 상황을 Log4j로 기록하는 스트리밍 리스너.

    signal_spark와 signal_path가 설정되면, 매 배치/유휴 시점마다
    S3 시그널 파일을 확인하여 스트리밍 쿼리를 graceful shutdown 합니다.
    """

    def __init__(
        self,
        signal_spark: SparkSession | None = None,
        signal_path: str | None = None,
    ):
        super().__init__()
        self._signal_spark = signal_spark
        self._signal_path = signal_path

    def _check_signal(self) -> None:
        if not self._signal_spark or not self._signal_path:
            return
        from utils.signal import check_stop_signal

        if check_stop_signal(self._signal_spark, self._signal_path):
            logger = SparkLoggerManager().get_logger()
            if logger:
                logger.warn(f"[Stream] Stop signal detected at {self._signal_path}. Stopping active queries.")
            for q in self._signal_spark.streams.active:
                q.stop()

    def onQueryStarted(self, event: QueryStartedEvent) -> None:
        logger = SparkLoggerManager().get_logger()
        if logger:
            logger.info(f"[Stream] Started: {event.name} (runId={event.runId})")

    def onQueryProgress(self, event: QueryProgressEvent) -> None:
        logger = SparkLoggerManager().get_logger()
        if not logger or not hasattr(event, "progress") or event.progress is None:
            return
        p = event.progress
        logger.info(
            f"[Stream] {p.name} batch={p.batchId} | rows={p.numInputRows} | in={p.inputRowsPerSecond:.1f}/s out={p.processedRowsPerSecond:.1f}/s"
        )
        self._check_signal()

    def onQueryIdle(self, event: QueryIdleEvent) -> None:
        self._check_signal()

    def onQueryTerminated(self, event: QueryTerminatedEvent) -> None:
        logger = SparkLoggerManager().get_logger()
        if not logger:
            return
        if event.exception:
            logger.error(f"[Stream] Terminated with error: {event.exception}")
        else:
            logger.info(f"[Stream] Terminated gracefully (id={event.id})")

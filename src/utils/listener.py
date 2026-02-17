from pyspark.sql.streaming.listener import (
    QueryIdleEvent,
    QueryProgressEvent,
    QueryStartedEvent,
    QueryTerminatedEvent,
    StreamingQueryListener,
)


class CustomStreamingListener(StreamingQueryListener):
    def onQueryStarted(self, event: QueryStartedEvent) -> None:
        # 쿼리 시작 시 실행 (Logging in English as requested)
        print("--- Query Started ---")
        print(f"ID: {event.id}")
        print(f"Name: {event.name}")
        print(f"Run ID: {event.runId}")

    def onQueryProgress(self, event: QueryProgressEvent) -> None:
        # 매 배치가 완료될 때마다 메트릭 출력
        progress = event.progress
        num_rows = progress.numInputRows
        input_rate = progress.inputRowsPerSecond

        print("--- Query Progress ---")
        print(f"Query Name: {progress.name}")
        print(f"Processed Rows: {num_rows}")
        print(f"Input Rate: {input_rate} rows/sec")

    def onQueryIdle(self, event: QueryIdleEvent) -> None:
        # 데이터가 없어 유휴 상태일 때 (Spark 3.5.0+)
        print("--- Query Idle ---")
        print(f"Query {event.id} is waiting for new data...")

    def onQueryTerminated(self, event: QueryTerminatedEvent) -> None:
        # 쿼리 종료 및 에러 처리
        print("--- Query Terminated ---")
        if event.exception:
            # 에러 발생 시 로그 출력
            print(f"Query terminated with exception: {event.exception}")
        else:
            print("Query terminated gracefully.")


# 리스너 등록 방법
# listener = CustomStreamingListener()
# spark.streams.addListener(listener)

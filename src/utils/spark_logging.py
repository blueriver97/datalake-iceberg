import threading


class SparkLoggerManager:
    _instance = None
    _lock = threading.Lock()
    _initialized = False
    _log_manager = None  # JVM LogManager 저장용

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def setup(self, spark):
        """Spark JVM을 통한 Log4j 초기화 및 매니저 로드"""
        if self._initialized:
            return

        with self._lock:
            if self._initialized:
                return

            try:
                jvm = spark._jvm
                self._log_manager = jvm.org.apache.logging.log4j.LogManager
                configurator = jvm.org.apache.logging.log4j.core.config.Configurator
                level = jvm.org.apache.logging.log4j.Level

                # 로그 레벨 설정
                configurator.setLevel("org.apache.spark", level.INFO)

                self._initialized = True
                print("SparkLoggerManager: Log4j 2 has been initialized.")
            except Exception as e:
                print(f"Error: Failed to setup Log4j 2. Detail: {str(e)}")

    def get_logger(self, name: str = ""):
        """JVM 로거 객체 반환"""
        if not self._initialized or self._log_manager is None:
            print("Warning: SparkLoggerManager not initialized. Call setup(spark) first.")
            return None

        # JVM의 getLogger 호출
        return self._log_manager.getLogger("org.apache.spark." + name if name else "org.apache.spark")


# 사용 예시
# manager = SparkLoggerManager()
# manager.setup(spark)
# logger = manager.get_logger("org.apache.spark")
# logger.info("This is an INFO level log from PySpark")

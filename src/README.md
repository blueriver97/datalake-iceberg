### Spark 애플리케이션

### kafka_to_iceberg.py

Kafka CDC 스트리밍 파이프라인. 토픽별 독립 스트림을 멀티스레드로 병렬 처리합니다.

- **Debezium CDC 처리**: Avro 역직렬화, Schema Registry 연동, 타입 변환
- **Iceberg 적재**: PK 기반 중복 제거 → MERGE INTO (upsert) + DELETE
- **Watermark 기록**: 토픽별 처리 상태를 `ops_bronze.cdc_watermark` 테이블에 append
- **Graceful Shutdown**: S3 시그널 파일 폴링으로 진행 중인 배치 완료 후 종료
- **동시성 제어**: `--concurrency` 인자와 `threading.Semaphore`로 제한

### kafka_to_s3.py

Kafka → S3 Parquet 스트리밍 파이프라인. 토픽 데이터를 S3에 Parquet 포맷으로 적재합니다.

### mysql_to_iceberg.py / sqlserver_to_iceberg.py

JDBC 배치 적재 파이프라인. 소스 테이블을 읽어 Iceberg 테이블로 전체 교체(RTAS)합니다.

- **파티셔닝**: auto_increment/identity 컬럼 자동 감지, bounds 쿼리로 분산 읽기
- **PK 처리**: INFORMATION_SCHEMA에서 PK 조회 → `id_iceberg` (MD5 해시) 생성
- **CHAR Trim**: CHAR 고정길이 공백 제거
- **Vault 연동**: DB 접속 정보를 HashiCorp Vault에서 런타임 조회

### mysql_to_parquet.py / mssql_to_parquet.py

JDBC → S3 Parquet 배치 적재. Iceberg 대신 S3에 Parquet 포맷으로 직접 저장합니다.

### parquet_to_iceberg.py

S3 Parquet → Iceberg 배치 적재. S3의 Parquet 파일을 읽어 Iceberg 테이블로 적재합니다.

### schema_validate.py

Iceberg 테이블 스키마 검증. 소스 DB 스키마와 Iceberg 테이블 스키마의 일관성을 확인합니다.

### iceberg_maintenance.py

Iceberg 테이블 최적화. CLI 인자만 사용하며 Settings 의존 없이 독립 실행됩니다.

1. `rewrite_data_files` (소파일 compaction)
2. `expire_snapshots` (오래된 스냅샷 만료)
3. `remove_orphan_files` (참조 해제된 파일 삭제)

### watermark_maintenance.py

CDC watermark 테이블의 주기적 정리. Settings 의존 없이 독립 실행됩니다.

1. 보관 기간 초과 레코드 삭제 (키별 최신 1건 보존)
2. `rewrite_data_files`, `expire_snapshots`, `remove_orphan_files`

### utils/

| 파일               | 설명                                                                                |
| ------------------ | ----------------------------------------------------------------------------------- |
| `settings.py`      | Pydantic BaseSettings + Vault 연동 (`env_file=".env"`, `env_nested_delimiter="__"`) |
| `database.py`      | JDBC 매니저 — `BaseDatabaseManager`, `MySQLManager`, `SQLServerManager`             |
| `listener.py`      | Spark Structured Streaming `BatchProgressListener` (시그널 감지)                    |
| `signal.py`        | S3 시그널 기반 graceful shutdown (`build_signal_path`, `check_stop_signal`)         |
| `spark_logging.py` | Log4j 기반 Spark 로거 (`SparkLoggerManager` 싱글턴)                                 |

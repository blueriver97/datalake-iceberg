# Spark 애플리케이션

## Kafka 스트리밍

### kafka_to_iceberg.py

Kafka CDC 스트리밍 파이프라인. 토픽별 독립 스트림을 멀티스레드로 병렬 처리합니다.

- **Debezium CDC 처리**: Avro 역직렬화, Schema Registry 연동, 타입 변환
- **Iceberg 적재**: PK 기반 중복 제거 → MERGE INTO (upsert) + DELETE
- **Watermark 기록**: 토픽별 처리 상태를 `ops_bronze.cdc_watermark` 테이블에 append
- **Graceful Shutdown**: S3 시그널 파일 폴링으로 진행 중인 배치 완료 후 종료
- **동시성 제어**: `--concurrency` 인자와 `threading.Semaphore`로 제한

```bash
spark-submit --py-files utils.zip kafka_to_iceberg.py \
  --dag-id "my_dag" --topics "prefix.schema.table1,prefix.schema.table2" \
  --concurrency 3 --env-file .env
```

### kafka_to_iceberg_stream.py

Kafka CDC 상시 실행 스트리밍 파이프라인. `kafka_to_iceberg.py`의 batch 모드를 while 루프로 감싸 라운드 기반으로 반복 처리합니다.

```bash
spark-submit --py-files utils.zip kafka_to_iceberg_stream.py \
  --dag-id "my_stream" --topics "prefix.schema.table1,..." \
  --concurrency 3 --round-interval 300 --env-file .env
```

### kafka_to_s3.py

Kafka → S3 Parquet 스트리밍 파이프라인. 토픽 데이터를 S3에 Parquet 포맷으로 적재합니다.

```bash
spark-submit --py-files utils.zip kafka_to_s3.py \
  --dag-id "my_dag" --topics "prefix.schema.table1,..." \
  --output-path s3a://bucket/data/raw/kafka --env-file .env
```

## JDBC 배치 적재

### mysql_to_iceberg.py / sqlserver_to_iceberg.py

JDBC 배치 적재 파이프라인. 소스 테이블을 읽어 Iceberg 테이블로 전체 교체(RTAS)합니다.

- **파티셔닝**: auto_increment/identity 컬럼 자동 감지, bounds 쿼리로 분산 읽기
- **PK 처리**: INFORMATION_SCHEMA에서 PK 조회 → `id_iceberg` (MD5 해시) 생성
- **CHAR Trim**: CHAR 고정길이 공백 제거
- **Vault 연동**: DB 접속 정보를 HashiCorp Vault에서 런타임 조회

```bash
# MySQL
spark-submit --py-files utils.zip mysql_to_iceberg.py \
  --table "db.table_name" --num_partition 8 --env-file .env

# SQL Server
spark-submit --py-files utils.zip sqlserver_to_iceberg.py \
  --table "db.dbo.table_name" --num_partition 8 --env-file .env
```

### mysql_to_parquet.py / mssql_to_parquet.py

JDBC → S3 Parquet 배치 적재. Iceberg 대신 S3에 Parquet 포맷으로 직접 저장합니다.

```bash
# MySQL
spark-submit --py-files utils.zip mysql_to_parquet.py \
  --table "db.table_name" --num_partition 8 --env-file .env

# SQL Server
spark-submit --py-files utils.zip mssql_to_parquet.py \
  --table "db.dbo.table_name" --num_partition 8 --env-file .env
```

### parquet_to_iceberg.py

S3 Parquet → Iceberg 배치 적재. S3의 Parquet 파일을 읽어 Iceberg 테이블로 적재합니다. `mysql_to_parquet` / `mssql_to_parquet`의 후속 단계로 사용됩니다.

```bash
spark-submit --py-files utils.zip parquet_to_iceberg.py \
  --table "db.table_name" --env-file .env
```

## 스키마 검증

### schema_validate.py

Iceberg 테이블 스키마 검증 및 주석 동기화. 소스 DB 스키마와 Iceberg 테이블의 컬럼 수/순서/타입/nullable/PK를 비교하고, 테이블·컬럼 주석을 동기화합니다.

```bash
spark-submit --py-files utils.zip schema_validate.py \
  --table "db.table_name" --env-file .env
```

## 유지보수

### iceberg_maintenance.py

Iceberg 테이블 유지보수. watermark 정리, compaction, orphan 파일 삭제를 순차 실행합니다.

1. **watermark 정리**: 키별 최신 1건 보존, retention 이전 레코드 삭제
2. **watermark 테이블 compaction**: `cdc_watermark`, `maintenance_watermark` 테이블 최적화
3. **orphan 파일 삭제**: 지정된 bronze 스키마의 `remove_orphan_files` 실행

```bash
spark-submit --py-files utils.zip iceberg_maintenance.py \
  --dag-id "iceberg_maintenance" \
  --schemas "store_bronze,order_bronze" \
  --retention-days 7 --orphan-older-than-days 3 --env-file .env
```

## 분석 도구

### analyze_spark_eventlog.py

Spark Event Log 분석 스크립트. zstd 압축된 이벤트 로그를 파싱하여 작업 실행 통계를 출력합니다.

```bash
python3 analyze_spark_eventlog.py <eventlog_dir>
```

---

### utils/

| 파일               | 설명                                                                                |
| ------------------ | ----------------------------------------------------------------------------------- |
| `settings.py`      | Pydantic BaseSettings + Vault 연동 (`env_file=".env"`, `env_nested_delimiter="__"`) |
| `database.py`      | JDBC 매니저 — `BaseDatabaseManager`, `MySQLManager`, `SQLServerManager`             |
| `cdc_pipeline.py`  | Kafka CDC → Iceberg 공통 처리 (Debezium 파싱, 타입 변환, upsert/delete)             |
| `iceberg.py`       | Iceberg 테이블 생성 유틸리티 (`create_or_replace_iceberg_table`)                    |
| `maintenance.py`   | Iceberg 유지보수 프로시저 실행기 (compaction, orphan cleanup, 테이블 추적)          |
| `watermark.py`     | 파이프라인/유지보수 상태 추적 (CDC watermark, maintenance watermark, purge)         |
| `cleansing.py`     | 데이터 클렌징 — CHAR 고정길이 공백 제거                                             |
| `listener.py`      | Spark Structured Streaming `BatchProgressListener` (시그널 감지)                    |
| `signal.py`        | S3 시그널 기반 graceful shutdown (`build_signal_path`, `check_stop_signal`)         |
| `spark_logging.py` | Log4j 기반 Spark 로거 (`SparkLoggerManager` 싱글턴)                                 |

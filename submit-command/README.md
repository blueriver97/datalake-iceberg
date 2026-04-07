### spark-submit 실행 가이드

### 파일 구조

```
submit-command/
├── env/                            # 앱별 환경변수 (.env)
│   ├── kafka_to_iceberg.env
│   ├── kafka_to_iceberg_stream.env
│   ├── kafka_to_s3.env
│   ├── mysql_to_iceberg.env
│   ├── sqlserver_to_iceberg.env
│   ├── mysql_to_parquet.env
│   ├── sqlserver_to_parquet.env
│   ├── parquet_to_iceberg.env
│   ├── schema_validate.env
│   ├── iceberg_maintenance.env
│   └── openlineage.env
├── kafka_to_iceberg.sh
├── ...
└── iceberg_maintenance.sh
```

### 환경변수 전달 방식 (YARN cluster mode)

```
env/<task>.env  →  --files <task>.env#.env  →  YARN 드라이버 working dir/.env
  → Settings(env_file=".env", env_nested_delimiter="__")  →  앱 설정 파싱
```

- `--files <task>.env#.env`: YARN이 파일을 드라이버에 `.env`로 rename하여 배포
- `AWS_PROFILE`은 `spark.yarn.appMasterEnv` / `spark.executorEnv`로 직접 전달 (Glue 사용 시 executor에서도 필요)
- 나머지 앱 설정 (VAULT**, DATABASE**, STORAGE**, KAFKA**, POLARIS\*\*): .env 파일로 전달

### 실행 방법

타겟 서버의 작업 디렉토리에 파일을 배치한 후 실행합니다.

```
<working-dir>/
├── utils.zip           # src/zip.sh로 생성
├── <app>.py            # src/ 아래 Python 파일
├── <app>.env           # submit-command/env/ 아래 env 파일
└── <app>.sh            # submit-command/ 아래 sh 파일
```

```bash
bash mysql_to_iceberg.sh
```

### env 파일 구성

각 `.env` 파일은 해당 Spark 앱이 필요로 하는 모든 환경변수를 포함합니다. `STORAGE__CATALOG_TYPE`에 따라 Glue 또는 Polaris 카탈로그를 선택합니다.

#### Glue Catalog (배치 적재)

```bash
VAULT__URL=http://vault.svc.internal:8200
VAULT__USERNAME=airflow
VAULT__PASSWORD=changeme
VAULT__SECRET_PATH=secret/data/user/database/local-mysql

DATABASE__TYPE=mysql

STORAGE__PROFILE=default
STORAGE__CATALOG=awsdatacatalog
STORAGE__CATALOG_TYPE=glue
STORAGE__BUCKET=your-bucket
STORAGE__DATA_PATH=/iceberg
```

#### Polaris Catalog (배치 적재)

```bash
VAULT__URL=http://vault.svc.internal:8200
VAULT__USERNAME=airflow
VAULT__PASSWORD=changeme
VAULT__SECRET_PATH=secret/data/user/database/local-mysql

DATABASE__TYPE=mysql

STORAGE__CATALOG=polaris
STORAGE__CATALOG_TYPE=polaris
STORAGE__BUCKET=your-bucket
STORAGE__DATA_PATH=/iceberg

POLARIS__URI=http://polaris.svc.internal:8181/api/catalog
POLARIS__OAUTH2_SERVER_URI=http://polaris.svc.internal:8181/api/catalog/v1/oauth/tokens
POLARIS__CREDENTIAL=root:polarisadmin
POLARIS__SCOPE=PRINCIPAL_ROLE:ALL
POLARIS__REALM=default
```

> `STORAGE__PROFILE`은 Glue 전용입니다. Polaris는 OAuth2 인증(`POLARIS__CREDENTIAL`)을 사용하므로 생략 가능합니다.

#### Parquet 적재 (mysql_to_parquet, sqlserver_to_parquet, parquet_to_iceberg)

Glue/Polaris 설정과 동일하되, `DATA_PATH`를 Parquet 저장 경로로 설정:

```bash
STORAGE__DATA_PATH=/data/parquet
```

#### Kafka 스트리밍 (kafka_to_iceberg, kafka_to_iceberg_stream, kafka_to_s3)

위 항목에 추가:

```bash
KAFKA__BOOTSTRAP_SERVERS=kafka:9092
KAFKA__SCHEMA_REGISTRY=http://schema-registry:8081
KAFKA__METRIC_NAMESPACE=metric_namespace
KAFKA__MAX_OFFSETS_PER_TRIGGER=1000000
KAFKA__STARTING_OFFSETS=earliest
```

#### 유지보수 (iceberg_maintenance)

배치 적재와 동일한 `STORAGE__*` 설정이 필요합니다.

### Settings 클래스 구조

```
Settings
├── VaultSettings (url, username, password, secret_path)
├── DatabaseSettings (type, host, port, user, password) ← Vault에서 주입
├── StorageSettings (profile?, catalog, catalog_type, bucket, data_path)
├── PolarisSettings (uri, oauth2_server_uri, credential, scope?, realm?) [Optional]
└── KafkaSettings (bootstrap_servers, schema_registry, ...) [Optional]
```

- `StorageSettings.profile`: Glue 전용, Polaris에서는 `None` (optional)
- `StorageSettings.catalog_type`: `"glue"` 또는 `"polaris"` (필수)
- `PolarisSettings`: `catalog_type=polaris`일 때만 필요, `scope`와 `realm`은 기본값 있음
- `KafkaSettings`: Kafka 앱에서만 필요, 없으면 `None`

### AWS Glue Catalog 관리 (CLI)

```bash
# 데이터베이스 목록
aws glue get-databases --profile default

# 특정 데이터베이스의 테이블 목록
aws glue get-tables --database-name store_bronze --profile default

# 특정 테이블 상세 정보
aws glue get-table --database-name store_bronze --name tb_lower --profile default

# 테이블 삭제
aws glue delete-table --database-name store_bronze --name tb_lower --profile default

# 테이블 여러 개 한번에 삭제
aws glue batch-delete-table --database-name store_bronze \
  --tables-to-delete tb_lower TB_UPPER TB_COMPOSITE_KEY --profile default

# 데이터베이스 삭제 (테이블이 비어있어야 함)
aws glue delete-database --name store_bronze --profile default
```

출력 포맷: `--output yaml` (AWS CLI v2), `--query "TableList[].Name" --output text` (v1/v2 공통)

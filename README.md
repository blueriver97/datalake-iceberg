# Datalake Iceberg

Apache Iceberg 기반 데이터레이크 적재 파이프라인. Spark on YARN으로 배치 및 스트리밍 작업을 수행하며, spark-admin 서버에서 직접 spark-submit으로 실행합니다.

## 프로젝트 구조

```
.
├── src/                            # Spark 애플리케이션 및 유틸리티
├── submit-command/                 # spark-submit 실행 스크립트 및 환경변수
├── tests/                          # Jupyter 노트북 테스트
├── scripts/                        # 개발 환경 셋업, Git 훅
├── docs/                           # 문서 (템플릿, gitflow, wily)
├── download/                       # JDBC 드라이버 등
└── pyproject.toml
```

자세한 내용은 각 디렉토리의 README를 참고하세요:

- [src/README.md](src/README.md) — Spark 애플리케이션 상세 설명
- [submit-command/README.md](submit-command/README.md) — 실행 방법, 환경변수 설정, Glue Catalog CLI

## 기술 스택

| 구분              | 기술                        | 버전  |
| ----------------- | --------------------------- | ----- |
| Compute           | Apache Spark on YARN        | 4.0.2 |
| Storage Format    | Apache Iceberg              | v2    |
| Catalog           | AWS Glue Data Catalog       | -     |
| Messaging         | Apache Kafka (Debezium CDC) | -     |
| Secret Management | HashiCorp Vault             | -     |
| Source DB         | MySQL, SQL Server           | -     |
| Runtime           | Python 3.12, Java 17        | -     |

## 빠른 시작

```bash
# 1. utils.zip 패키징
cd src && bash zip.sh

# 2. 타겟 서버에 파일 배포 (utils.zip, *.py, *.env, *.sh)

# 3. env 파일 편집 후 실행
bash mysql_to_iceberg.sh
```

## 라이선스

이 프로젝트는 Apache License 2.0을 따릅니다.

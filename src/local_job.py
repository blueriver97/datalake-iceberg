"""
MySQL → Iceberg 배치 적재 파이프라인

JDBC로 MySQL 테이블을 읽어 Iceberg 테이블로 전체 교체(RTAS)한다.

실행:
  spark-submit --py-files utils.zip mysql_to_iceberg.py \
    --service <service> --table "db.table_name" --num_partition 8 --env-file .env
"""

import argparse

# --- Import common modules ---
from utils.settings import Settings
from utils.spark import create_spark_session

if __name__ == "__main__":
    # 로컬 실행: 인자 없이 `python mysql_to_iceberg.py` 로 돌리면 아래 기본값을 sys.argv에 주입
    # spark-submit으로 실행할 때는 기존 CLI 인자가 유지되므로 분기가 타지 않는다
    import os
    import sys

    tables = """""".split()
    for table in tables:
        if len(sys.argv) == 1:
            os.environ["AWS_PROFILE"] = "default"
            sys.argv += ["--service", "", "--table", "", "--num_partition", "1"]

        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--service",
            type=str.lower,
            required=True,
            help="서비스 영문 식별자 (Glue Catalog Database prefix, 소문자로 정규화)",
        )
        parser.add_argument("--table", type=str)
        parser.add_argument("--num_partition", type=int)
        parser.add_argument("--env-file", type=str, default=".env", help="환경 설정 파일 경로 (기본값: .env)")
        args = parser.parse_args()
        settings = Settings(_env_file=args.env_file)

        spark = create_spark_session(
            "mysql_to_iceberg",
            settings,
            extra_configs={
                "spark.sql.optimizer.excludedRules": "org.apache.spark.sql.catalyst.optimizer.SimplifyCasts",
            },
        )
        args.__setattr__("table", table)
        # main(spark, settings, args)
        # spark.stop()

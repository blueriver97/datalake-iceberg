#!/usr/bin/env bash
set -euo pipefail

NUM_PARTITION=1

TABLES=(
  "store.tb_lower"
  "store.TB_UPPER"
  "store.TB_COMPOSITE_KEY"
)

for TABLE in "${TABLES[@]}"; do
  echo "=== Submitting: ${TABLE} ==="
  spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --name "glue_mysql_to_parquet.${TABLE}" \
    --py-files utils.zip \
    --files mysql_to_parquet.env#.env \
    --conf spark.yarn.maxAppAttempts=1 \
    --conf spark.yarn.appMasterEnv.AWS_PROFILE=default \
    --conf spark.executorEnv.AWS_PROFILE=default \
    --conf spark.driver.cores=1 \
    --conf spark.driver.memory=1G \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=1G \
    --conf spark.executor.instances=1 \
    mysql_to_parquet.py \
    --table "${TABLE}" \
    --num_partition "${NUM_PARTITION}"
done

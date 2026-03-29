#!/usr/bin/env bash
set -euo pipefail

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
    --name "glue_schema_validate.${TABLE}" \
    --py-files utils.zip \
    --files schema_validate.env#.env \
    --conf spark.yarn.maxAppAttempts=1 \
    --conf spark.yarn.appMasterEnv.AWS_PROFILE=default \
    --conf spark.executorEnv.AWS_PROFILE=default \
    --conf spark.driver.cores=1 \
    --conf spark.driver.memory=1G \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=1G \
    --conf spark.executor.instances=1 \
    schema_validate.py \
    --table "${TABLE}"
done

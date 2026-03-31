#!/usr/bin/env bash
set -euo pipefail

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name glue_kafka_to_iceberg \
  --py-files utils.zip \
  --files kafka_to_iceberg.env#.env \
  --conf spark.yarn.maxAppAttempts=1 \
  --conf spark.yarn.appMasterEnv.AWS_PROFILE=default \
  --conf spark.executorEnv.AWS_PROFILE=default \
  --conf spark.driver.cores=1 \
  --conf spark.driver.memory=1G \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=2G \
  --conf spark.executor.instances=2 \
  kafka_to_iceberg.py \
  --dag-id glue_kafka_to_iceberg \
  --topics "local.store.tb_lower,local.store.TB_UPPER,local.store.TB_COMPOSITE_KEY" \
  --concurrency 3 \
  --scheduled-at "2026-03-31T00:00:00+00:00"

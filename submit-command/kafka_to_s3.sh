#!/usr/bin/env bash
set -euo pipefail

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name glue_kafka_to_s3 \
  --py-files utils.zip \
  --files kafka_to_s3.env#.env \
  --conf spark.yarn.maxAppAttempts=1 \
  --conf spark.yarn.appMasterEnv.AWS_PROFILE=default \
  --conf spark.executorEnv.AWS_PROFILE=default \
  --conf spark.driver.cores=1 \
  --conf spark.driver.memory=1G \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=2G \
  --conf spark.executor.instances=2 \
  kafka_to_s3.py \
  --dag-id glue_kafka_to_s3 \
  --topics "local.store.tb_lower,local.store.TB_UPPER,local.store.TB_COMPOSITE_KEY" \
  --output-path "s3a://blueriver-datalake/data/raw/kafka"

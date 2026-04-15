#!/usr/bin/env bash
set -euo pipefail

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name glue_iceberg_maintenance \
  --py-files utils.zip \
  --files iceberg_maintenance.env#.env \
  --conf spark.yarn.maxAppAttempts=1 \
  --conf spark.yarn.appMasterEnv.AWS_PROFILE=default \
  --conf spark.executorEnv.AWS_PROFILE=default \
  --conf spark.driver.cores=1 \
  --conf spark.driver.memory=1G \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=1G \
  --conf spark.executor.instances=1 \
  iceberg_maintenance.py \
  --dag-id "glue_iceberg_maintenance" \
  --schemas "local_store,di_ops" \
  --orphan-older-than-days 3 \
  --watermark-retention-days 14 \
  --compaction-interval 86400 \
  --orphan-interval 86400

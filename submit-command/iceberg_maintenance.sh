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
  --catalog "awsdatacatalog" \
  --warehouse "s3a://blueriver-datalake/data/iceberg" \
  --schemas "store_bronze" \
  --retention-days 1 \
  --retain-last 5 \
  --target-file-size 134217728 \
  --min-file-size 100663296

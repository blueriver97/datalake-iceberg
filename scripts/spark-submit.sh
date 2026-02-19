# spark-submit.sh 01.sample.py

spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --queue default \
    --driver-cores 1 \
    --driver-memory 2G \
    --executor-cores 1 \
    --executor-memory 2G \
    --num-executors 4 \
    --conf spark.yarn.maxAppAttempts=1 \
    --conf spark.network.timeout=300s \
    --conf spark.files=prod.env \
    --name $(echo $1 | cut -d '.' -f 2) \
    $1

#    --conf spark.yarn.appMasterEnv.INCLUDE_TABLE= \
#    --conf spark.yarn.appMasterEnv.CATALOG=glue_catalog \
#    --conf spark.yarn.appMasterEnv.PARQUET_S3_ROOT_PATH=s3a://hunet-di-data-lake-prod/raw_full_loaded \
#    --conf spark.yarn.appMasterEnv.ICEBERG_S3_ROOT_PATH=s3a://hunet-di-data-lake-prod/iceberg \

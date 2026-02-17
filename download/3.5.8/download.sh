#!/bin/bash
# .env 파일 읽기

# Package Version (Spark 버전 내 hadoop-client 버전에 따라 hadoop 버전 결정 필요)
# .env 파일 읽기
while IFS='=' read -r key value; do
    if [[ $key == "SPARK_VERSION" ]]; then
        SPARK_VERSION=$value
        SPARK_SHORT_VERSION=$(echo "$value" | awk -F. '{print $1 "." $2}')
        echo "$key=$value"
    elif [[ $key == "ICEBERG_VERSION" ]]; then
        ICEBERG_VERSION=$value
        echo "$key=$value"
    elif [[ $key == "HADOOP_VERSION" ]]; then
        HADOOP_VERSION=$value
        echo "$key=$value"
    fi
done < .env

# kafka-clients 버전은 카프카 버전을 따라감.
# (2026-02-17) log4j-slf4j-impl 버전은 Spark 내 log4j 버전을 따라감. (spark 3.5.8 기준 2.20.0)
# (2026-02-17) aws-java-sdk-bundle-1.12.262.jar (Hadoop 3.3.4 기준)
declare -a jar_urls=(
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_SHORT_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_SHORT_VERSION}_2.12-${ICEBERG_VERSION}.jar"
    "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar"
    "https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/${SPARK_VERSION}/spark-streaming-kafka-0-10_2.12-${SPARK_VERSION}.jar"
    "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar"
    "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/${SPARK_VERSION}/spark-avro_2.13-${SPARK_VERSION}.jar"
    "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar"
    "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar"
    "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.2.0.jre8/mssql-jdbc-12.2.0.jre8.jar"
    "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30.jar"
    "https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-slf4j-impl/2.20.0/log4j-slf4j-impl-2.20.0.jar"
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar"
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
    # Polaris
    "https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.3.0-incubating/polaris-spark-3.5_2.12-1.3.0-incubating.jar"
    "https://repo1.maven.org/maven2/org/apache/polaris/polaris-core/1.3.0-incubating/polaris-core-1.3.0-incubating.jar"
    # OpenLineage
    "https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/1.43.0/openlineage-spark_2.12-1.43.0.jar"
    "https://repo1.maven.org/maven2/org/apache/spark/spark-hive_2.12/${SPARK_VERSION}/spark-hive_2.12-${SPARK_VERSION}.jar"
    "https://repo1.maven.org/maven2/org/apache/hive/hive-exec/${SPARK_VERSION}/hive-exec-${SPARK_VERSION}.jar"
)


declare -a jars=()

for url in "${jar_urls[@]}"; do
    filename=$(basename "$url")
    jars+=("$filename")
done

for (( i=0; i<${#jar_urls[@]}; i++ )); do
    if [ -f "${jars[$i]}" ]; then
        echo "${jars[$i]} ... existed"
    else
        wget "${jar_urls[$i]}" -O "${jars[$i]}"
    fi
done

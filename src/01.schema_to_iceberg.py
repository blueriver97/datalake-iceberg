import os

import pyspark.sql.types as T
from pyspark.sql import SparkSession

from utils.database import (
    DatabaseType,
    convert_db_type_to_spark,
    get_primary_keys,
    get_table_schema_info,
)

# --- Import common modules ---
from utils.settings import Settings
from utils.spark_logging import SparkLoggerManager


def main(spark: SparkSession, config: Settings) -> None:
    """
    Creates Iceberg tables based on the schema of the source database.
    """
    logger_manager = SparkLoggerManager()
    logger_manager.setup(spark)
    logger = logger_manager.get_logger(__name__)

    logger.info("Starting Iceberg table creation from schema.")
    logger.info(f"Target tables: {config.job.tables}")

    # Note. Retrieve schema and primary key information from the database.
    table_schemas: dict[str, dict[str, str]] = get_table_schema_info(spark, config)
    primary_keys: dict[str, list[str]] = get_primary_keys(spark, config)

    for table_name in config.job.tables:
        try:
            # MSSQL은 'db.dbo.table' 형식을 사용하므로 이를 고려하여 파싱합니다.
            # Parse table name considering MSSQL uses 'db.dbo.table' format.
            parts = table_name.split(".")

            if len(parts) == 3 and config.DB_TYPE == DatabaseType.MSSQL:
                schema, _, table = parts
            else:
                schema, table = parts

            bronze_schema = f"{schema.lower()}_bronze"
            target_table = table.lower()
            full_table_name = f"{config.iceberg.catalog}.{bronze_schema}.{target_table}"

            # Note. Create the database if it does not exist.
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.iceberg.catalog}.{bronze_schema}")

            # Note. Skip if the table already exists.
            if spark.catalog.tableExists(full_table_name):
                logger.info(f"Table '{full_table_name}' already exists. Skipping.")
                continue

            # Note. Generate the Spark schema for the new Iceberg table.
            schema_info = table_schemas.get(table_name, {})
            if not schema_info:
                logger.warn(f"Schema information not found for '{table_name}'. Skipping.")
                continue

            pk_cols = primary_keys.get(table_name)

            struct_fields = [
                T.StructField("last_applied_date", T.TimestampType(), True),
                *[
                    T.StructField(col, convert_db_type_to_spark(col_type, config.DB_TYPE), True)
                    for col, col_type in schema_info.items()
                ],
            ]
            if pk_cols:
                struct_fields.append(T.StructField("id_iceberg", T.StringType(), True))

            final_schema = T.StructType(struct_fields)
            empty_df = spark.createDataFrame([], final_schema)

            # Note. Create the Iceberg table with specified properties.
            (
                empty_df.writeTo(full_table_name)
                .using("iceberg")
                .tableProperty("location", f"{config.iceberg.s3_root_path}/{bronze_schema}/{target_table}")
                .tableProperty("format-version", "2")
                .tableProperty("write.metadata.delete-after-commit.enabled", "true")
                .tableProperty("write.metadata.previous-versions-max", "5")
                .tableProperty("history.expire.max-snapshot-age-ms", "86400000")  # 1 day
                .createOrReplace()
            )
            logger.info(f"Successfully created table: {full_table_name}")

        except Exception as e:
            logger.error(f"Failed to create table for '{table_name}': {e}")


if __name__ == "__main__":
    # 1. Load settings
    settings = Settings()

    # 2. Set AWS profile from settings
    os.environ["AWS_PROFILE"] = settings.aws.profile

    # 3. Create a Spark session
    spark_session = (
        SparkSession.builder.appName("schema_to_iceberg")
        .config("spark.sql.defaultCatalog", settings.iceberg.catalog)
        .config(f"spark.sql.catalog.{settings.iceberg.catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{settings.iceberg.catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{settings.iceberg.catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{settings.iceberg.catalog}.warehouse", settings.iceberg.s3_root_path)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.AwsCredentialsProvider",
        )
        .getOrCreate()
    )

    # 4. Run the main logic
    main(spark_session, settings)
    spark_session.stop()

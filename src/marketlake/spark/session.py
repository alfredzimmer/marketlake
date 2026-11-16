"""SparkSession wired to Kafka and the `lake` Iceberg REST catalog."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

SPARK_VERSION = "4.0.1"
ICEBERG_VERSION = "1.10.1"

PACKAGES = [
    f"org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_VERSION}",
    f"org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:{ICEBERG_VERSION}",
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
]


def spark_conf() -> dict[str, str]:
    """Spark settings for the `lake` catalog. Single source of truth: used by
    `build()` and rendered into the dbt profile (`marketlake.transform`)."""
    return {
        "spark.master": os.environ.get("SPARK_MASTER", "local[4]"),
        "spark.jars.packages": ",".join(PACKAGES),
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.lake": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.lake.type": "rest",
        "spark.sql.catalog.lake.uri": os.environ.get("ICEBERG_REST_URI", "http://localhost:8181"),
        "spark.sql.catalog.lake.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.lake.s3.endpoint": os.environ.get("S3_ENDPOINT", "http://localhost:8333"),
        "spark.sql.catalog.lake.s3.path-style-access": "true",
        "spark.sql.catalog.lake.s3.access-key-id": "marketlake",
        "spark.sql.catalog.lake.s3.secret-access-key": "marketlake",
        "spark.sql.catalog.lake.client.region": "us-east-1",
        "spark.sql.defaultCatalog": "lake",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": "8",
    }


def build(app: str) -> SparkSession:
    builder = SparkSession.builder.appName(app)
    for key, value in spark_conf().items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

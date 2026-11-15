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


def build(app: str) -> SparkSession:
    catalog_uri = os.environ.get("ICEBERG_REST_URI", "http://localhost:8181")
    s3_endpoint = os.environ.get("S3_ENDPOINT", "http://localhost:8333")
    spark = (
        SparkSession.builder.appName(app)
        .master(os.environ.get("SPARK_MASTER", "local[4]"))
        .config("spark.jars.packages", ",".join(PACKAGES))
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "rest")
        .config("spark.sql.catalog.lake.uri", catalog_uri)
        .config("spark.sql.catalog.lake.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lake.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.lake.s3.path-style-access", "true")
        .config("spark.sql.catalog.lake.s3.access-key-id", "marketlake")
        .config("spark.sql.catalog.lake.s3.secret-access-key", "marketlake")
        .config("spark.sql.catalog.lake.client.region", "us-east-1")
        .config("spark.sql.defaultCatalog", "lake")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

import os
import time

import pytest

# Run every test under a non-UTC local zone: PySpark converts timestamps to
# naive local datetimes, so code that round-trips them through Python only
# misbehaves off UTC (see #1).
os.environ["TZ"] = "America/New_York"
time.tzset()


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    from marketlake.spark.session import ICEBERG_VERSION

    warehouse = tmp_path_factory.mktemp("warehouse")
    s = (
        SparkSession.builder.master("local[2]").appName("tests")
        .config("spark.jars.packages",
                f"org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:{ICEBERG_VERSION}")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", str(warehouse))
        .config("spark.sql.defaultCatalog", "lake")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.iceberg.check-nullability", "false")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()

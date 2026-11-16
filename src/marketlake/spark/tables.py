"""Iceberg table definitions for silver and gold layers.

Run: uv run python -m marketlake.spark.tables
"""

from __future__ import annotations

from pyspark.sql import SparkSession

# Coinbase quotes prices/sizes with <= 8-10 decimal places; decimal(28,10)
# leaves headroom for prices up to 1e18 while keeping products of two values
# inside Spark's 38-digit decimal limit without silent scale loss.
DDL = [
    "CREATE NAMESPACE IF NOT EXISTS silver",
    "CREATE NAMESPACE IF NOT EXISTS gold",
    """
    CREATE TABLE IF NOT EXISTS silver.trades (
        exchange        STRING    NOT NULL,
        symbol          STRING    NOT NULL,
        trade_id        BIGINT    NOT NULL,
        price           DECIMAL(28,10) NOT NULL,
        size            DECIMAL(28,10) NOT NULL,
        taker_side      STRING    NOT NULL,
        event_ts        TIMESTAMP NOT NULL,
        ingest_ts       TIMESTAMP NOT NULL,
        kafka_partition INT,
        kafka_offset    BIGINT
    )
    USING iceberg
    PARTITIONED BY (days(event_ts), exchange)
    TBLPROPERTIES (
        'format-version' = '2',
        'write.merge.mode' = 'merge-on-read',
        'write.distribution-mode' = 'hash',
        'write.target-file-size-bytes' = '134217728'
    )
    """,
    "ALTER TABLE silver.trades WRITE ORDERED BY symbol, trade_id",
    # AQE coalesces the write shuffle toward this size; also applied to tables
    # created before it was added.
    "ALTER TABLE silver.trades SET TBLPROPERTIES ('write.spark.advisory-partition-size-bytes' = '134217728')",
    """
    CREATE TABLE IF NOT EXISTS gold.bars_1m (
        exchange     STRING    NOT NULL,
        symbol       STRING    NOT NULL,
        window_start TIMESTAMP NOT NULL,
        window_end   TIMESTAMP NOT NULL,
        open         DECIMAL(28,10),
        high         DECIMAL(28,10),
        low          DECIMAL(28,10),
        close        DECIMAL(28,10),
        volume       DECIMAL(38,10),
        notional     DECIMAL(38,10),
        vwap         DECIMAL(28,10),
        trade_count  BIGINT,
        buy_volume   DECIMAL(38,10),
        sell_volume  DECIMAL(38,10)
    )
    USING iceberg
    PARTITIONED BY (days(window_start))
    TBLPROPERTIES ('format-version' = '2')
    """,
]


def create_all(spark: SparkSession) -> None:
    for stmt in DDL:
        spark.sql(stmt)


if __name__ == "__main__":
    from marketlake.spark.session import build

    spark = build("marketlake-ddl")
    create_all(spark)
    spark.sql("SHOW TABLES IN silver").show()
    spark.sql("SHOW TABLES IN gold").show()

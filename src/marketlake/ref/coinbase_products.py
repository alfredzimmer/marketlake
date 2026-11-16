"""Coinbase product metadata -> Iceberg ref.coinbase_products (full refresh).

Each run replaces the table with the current product list. History (status
changes, tick-size changes, delistings) is kept by the dbt snapshot
`snap_coinbase_products`, not here.

Run: uv run python -m marketlake.ref.coinbase_products
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import httpx
from pyspark.sql import SparkSession

URL = "https://api.exchange.coinbase.com/products"
TABLE = "ref.coinbase_products"

COLUMNS = {
    "id": "STRING", "base_currency": "STRING", "quote_currency": "STRING",
    "quote_increment": "STRING", "base_increment": "STRING", "min_market_funds": "STRING",
    "status": "STRING", "status_message": "STRING", "trading_disabled": "BOOLEAN",
    "post_only": "BOOLEAN", "limit_only": "BOOLEAN", "cancel_only": "BOOLEAN",
    "auction_mode": "BOOLEAN", "fx_stablecoin": "BOOLEAN",
}

DDL = [
    "CREATE NAMESPACE IF NOT EXISTS ref",
    f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        {", ".join(f"{c} {t}" for c, t in COLUMNS.items())},
        fetched_at TIMESTAMP NOT NULL
    ) USING iceberg TBLPROPERTIES ('format-version' = '2')
    """,
]

log = logging.getLogger("ref.coinbase_products")


def fetch(client: httpx.Client | None = None) -> list[dict]:
    client = client or httpx.Client(timeout=30)
    resp = client.get(URL, headers={"User-Agent": "marketlake"})
    resp.raise_for_status()
    products = resp.json()
    if not isinstance(products, list) or not products:
        raise ValueError(f"unexpected products payload: {str(products)[:200]}")
    return products


def load(spark: SparkSession, products: list[dict], fetched_at: datetime | None = None) -> int:
    for stmt in DDL:
        spark.sql(stmt)
    fetched_at = fetched_at or datetime.now(UTC)
    rows = [{c: p.get(c) for c in COLUMNS} | {"fetched_at": fetched_at} for p in products]
    schema = ", ".join(f"{c} {t}" for c, t in COLUMNS.items()) + ", fetched_at TIMESTAMP"
    spark.createDataFrame(rows, schema).writeTo(TABLE).overwritePartitions()
    return len(rows)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    from marketlake.spark.session import build

    n = load(build("marketlake-ref-products"), fetch())
    log.info("loaded %d products into %s", n, TABLE)


if __name__ == "__main__":
    main()

# marketlake

A streaming lakehouse for crypto market data: exchange websockets → Kafka → stream processing → Iceberg → dbt → SQL.

## Architecture

```
Coinbase / Hyperliquid / Binance websockets
   → ingestors (reconnect, trade-id gap detection)          [phase 1 ✅ coinbase]
   → Redpanda: trades.raw, ingest.gaps                      [phase 1 ✅]
   → bronze Parquet sink (at-least-once, offset-tracked)    [phase 1 ✅]
   → Spark Structured Streaming                             [phase 2 ✅]
        silver.trades  deduped, typed, idempotent MERGE
        gold.bars_1m   1m OHLCV + VWAP, event-time watermark
   → Iceberg (REST catalog on Postgres) over S3 (SeaweedFS) [phase 2 ✅]
   → dbt on Spark/Iceberg                                   [phase 3 ✅]
        fct_bars_1d, dim_symbol (SCD2), rpt_bar_reconciliation
        data quality tests (OHLC, outliers, gaps, reconciliation)
   → Dagster orchestration                                  [phase 3 ✅]
   → Trino + Grafana dashboards                             [demo ✅]
   → REST backfill of gaps, compaction, reconciliation      [phase 4 ✅]
   → exactly-once proof                                     [phase 4]
```

## Quickstart

Requires Docker, [uv](https://docs.astral.sh/uv/), and Java 17+ (for Spark).

```bash
docker compose up -d                              # Redpanda, SeaweedFS, Postgres, Iceberg REST
uv sync
uv run python -m marketlake.ingest.coinbase       # websocket → trades.raw
uv run python -m marketlake.ingest.backfill       # ingest.gaps → REST → trades.raw
uv run python -m marketlake.sink.bronze           # trades.raw → data/bronze/ (Parquet)
uv run python -m marketlake.spark.stream_trades   # trades.raw → silver.trades, gold.bars_1m
uv run python -m marketlake.ref.coinbase_products # product metadata → ref.coinbase_products
uv run python -m marketlake.transform build       # dbt: snapshot, models, tests
uv run dagster dev                                # orchestration UI on :3000 (turn schedules on there)
open http://localhost:3001                        # Grafana dashboards (starts with `docker compose up`)
uv run pytest
```

Spark jobs can also run inside the compose network: `docker compose run --rm spark python -m marketlake.spark.<job>`. Use this for anything that scans thousands of files, since Docker Desktop's port proxy resets S3 connections under that load ([#18](../../issues/18)).

| service | port | |
|---|---|---|
| Redpanda (Kafka API) | 19092 | |
| Redpanda Console | 8080 | browse topics |
| Iceberg REST catalog | 8181 | |
| SeaweedFS S3 | 8333 | bucket `warehouse` |
| Trino | 8090 | catalog `lake` (`docker compose exec trino trino --catalog lake`) |
| Grafana | 3001 | dashboards; anonymous view, admin / `marketlake` |


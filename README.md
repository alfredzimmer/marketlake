# marketlake

A streaming lakehouse for crypto market data: exchange websockets → Kafka → stream processing → Iceberg → dbt → SQL.

## Architecture (target)

```
Coinbase / Hyperliquid / Binance websockets
   → ingestors (reconnect, trade-id gap detection)          [phase 1 ✅ coinbase]
   → Redpanda: trades.raw, ingest.gaps                      [phase 1 ✅]
   → bronze Parquet sink (at-least-once, offset-tracked)    [phase 1 ✅]
   → Flink/Spark: normalize, OHLCV, VWAP, watermarks        [phase 2]
   → Iceberg on MinIO (bronze/silver/gold)                  [phase 2]
   → dbt models + data quality checks, Dagster/Airflow      [phase 3]
   → exactly-once proof, REST backfill, compaction,
     batch-vs-stream reconciliation                         [phase 4]
```

## Quickstart

```bash
docker compose up -d                              # Redpanda + console (http://localhost:8080)
uv sync
uv run python -m marketlake.ingest.coinbase       # websocket → trades.raw
uv run python -m marketlake.sink.bronze           # trades.raw → data/bronze/
uv run pytest
```

## Design decisions

- **Partition key = `exchange:symbol`.** Keeps each symbol's trades in order within one partition, which gap detection and stateful windows downstream rely on.
- **Idempotent producer (`enable.idempotence`, `acks=all`).** Producer retries can't duplicate records in the log.
- **Prices as decimal strings.** No float round-trips at ingest; typing happens in silver.
- **Taker side, not maker side.** Coinbase reports the maker side. We store the aggressor side, the convention for order-flow metrics.
- **Gap detection on `trade_id`.** Coinbase trade ids are contiguous per product. The detector's state survives reconnects, so a disconnect window shows up as a gap event on `ingest.gaps`, which the phase 4 backfiller consumes.
- **Bronze sink is at-least-once by design.** Files are fsynced and atomically renamed *before* offsets are committed. File names encode the Kafka partition and offset range (`p1-733-1255.parquet`), so a replay is auditable. Deduplication on `(exchange, symbol, trade_id)` happens in silver.

## Results log

| date | run | result |
|---|---|---|
| 2026-11-15 | 45s live, BTC/ETH/SOL-USD | 1,711 trades (~43/s); 0 dupes, 0 id holes in bronze; committed offsets == rows written |

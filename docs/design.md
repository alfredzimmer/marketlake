# Design

The reasoning behind each layer. The README has the one-line version.

## Ingest

- **Partition key = `exchange:symbol`, murmur2 partitioner.** Keeps each symbol's trades in order within one partition, which gap detection and stateful windows downstream rely on. librdkafka defaults to CRC32, so the ingestor sets `murmur2_random` to place keys exactly like Java clients, Kafka Connect and `rpk` do ([#5](../../../issues/5)).
- **Idempotent producer (`enable.idempotence`, `acks=all`).** Producer retries can't duplicate records in the log.
- **No silent loss when Kafka is down.** Records that fail delivery (timeout, full local queue, still queued at shutdown) go to an fsynced local journal (`data/spill/`). They are replayed once the broker accepts writes, and the journal batch is deleted only after every record is acknowledged. Replayed records never re-spill, so the journal can't grow on retries during an outage.
- **Prices as decimal strings.** No float round-trips at ingest; typing happens in silver.
- **Taker side, not maker side.** Coinbase reports the maker side. We store the aggressor side, the convention for order-flow metrics.
- **Gap detection on `trade_id`.** Coinbase trade ids are contiguous per product. The detector's state survives reconnects, so a disconnect window shows up as a gap event on `ingest.gaps`, which the phase 4 backfiller consumes. It also survives restarts: the ingestor saves the highest *durable* trade id per symbol (acked by Kafka or in the spill journal) to `data/state/` every 5s and on shutdown, and seeds the detector from it. Saving durable ids rather than seen ids matters: trades still queued in the producer when the process is killed fall inside the next run's gap and get backfilled ([#8](../../../issues/8)).
- **Gaps are backfilled from REST.** The backfiller pages Coinbase's `/trades` endpoint over each gap, produces the trades to `trades.raw`, and commits the gap only after Kafka acks them (a replay is deduped in silver). Each gap gets an audit record on `ingest.backfills` (`complete` / `partial` / `skipped` / `failed`).
- **Bronze never rejects a record.** Anything that fails parsing or validation (including partition-path values like `exchange="../.."`) is written to `exchange=_unparsed/` with its raw value and parse error. A poison pill can't stall the sink, and nothing is lost.
- **Bronze sink is at-least-once by design.** Files are fsynced and atomically renamed *before* offsets are committed. File names encode the Kafka partition and offset range (`p1-733-1255.parquet`), so a replay is auditable. Deduplication on `(exchange, symbol, trade_id)` happens in silver.

## Phase 2: stream processing

- **Silver writes are idempotent (`MERGE ... WHEN NOT MATCHED INSERT`).** Spark commits the Iceberg snapshot and then the checkpoint. A crash between the two replays the micro-batch, and the MERGE inserts nothing the second time. The result is exactly-once table contents on top of at-least-once delivery. The MERGE bounds are rendered inside Spark; collecting them as Python datetimes once shifted them by the local UTC offset and broke this ([#1](../../../issues/1)). The MERGE condition also bounds target `event_ts` to the batch's min/max, so Iceberg prunes to the touched day partitions instead of scanning the whole table.
- **Silver has no watermark; gold does.** Silver accepts every trade, however late. Gold bars use a 30s event-time watermark in append mode, so each bar is written once, after its window closes. Trades later than the watermark are dropped from the *streaming* bars and counted per batch (`late_dropped`). Phase 4 reconciles the bars against a batch recompute from silver.
- **One gold query per exchange.** Spark keeps one watermark per query, driven by the max event time across all its rows. With a shared query, a fast exchange would push the watermark forward and a lagging feed's trades would be dropped as late. Each exchange (`--exchanges`, default `coinbase`) gets its own query, checkpoint (`gold_bars_1m_<exchange>`) and watermark. The pre-split `gold_bars_1m` checkpoint is renamed to coinbase's on startup: it keeps the query id, which the Iceberg sink's replay check depends on, so no bars are rewritten. Backfilled trades are late by nature, whatever the watermark. They reach silver, and the batch layers (`fct_bars_1d`, `rpt_bar_reconciliation`) recompute from silver ([#9](../../../issues/9)).
- **Dedupe in both layers.** Silver dedupes on the MERGE key. Gold uses `dropDuplicatesWithinWatermark`, which keeps dedupe state bounded by the watermark instead of growing forever. The same `bars_1m()` function runs on batch DataFrames (falling back to a full `dropDuplicates`), so stream and batch share one definition of a bar.
- **Open/close by `trade_id`, not arrival order.** `min_by(price, trade_id)` / `max_by(price, trade_id)` stay correct when events arrive out of order inside a window.
- **Decimal precision is deliberate.** `decimal(28,10) * decimal(28,10)` overflows 38 digits, and Spark silently cuts the result scale to 6. The notional is computed from `decimal(20,10)` operands, which gives a `decimal(38,17)` product.
- **`try_cast` on parse.** Spark 4 defaults to ANSI mode, where one malformed price throws and fails the query. A unit test caught this.
- **Rejects are quarantined, not dropped.** Each record is tagged with a `reject_reason` covering every NOT NULL silver column. A single null in a required Iceberg column fails the whole write, so a partial check is a crash waiting to happen. Rejects go to `quarantine.trades` with their raw bytes, merged on Kafka partition/offset so replays don't duplicate them, and counts are logged per batch.

## Table maintenance

`uv run python -m marketlake.spark.maintenance {compact|expire|orphans|all}` runs over silver, gold and quarantine, and is safe while the stream is writing.

- **compact:** `rewrite_data_files` (bin-pack to 128 MB, `partial-progress` so a conflict with a streaming commit only retries one file group) + `rewrite_manifests`.
- **expire:** snapshots older than 24h, but **always keeps the last 100**. The Iceberg streaming sink makes gold replays idempotent by finding its last epoch id in snapshot history. If compaction commits after the stream and expiry keeps only that snapshot, the marker is gone and a replayed epoch is appended twice. A test reproduces this.
- **orphans:** files older than 3 days that no snapshot references (younger files may belong to an in-flight commit). Uses `prefix_listing` so it lists through Iceberg's S3FileIO; the default Hadoop listing has no `s3://` filesystem.

## Phase 3: dbt

dbt-spark's in-process `session` method runs on the same Spark/Iceberg setup. `marketlake.transform` renders `profiles.yml` from `spark_conf()`, so dbt and the Spark jobs can't drift apart on catalog or S3 settings.

| model | what |
|---|---|
| `fct_bars_1d` | Daily OHLCV/VWAP per symbol. Incremental MERGE that **recomputes the trailing 2 days** on every run, so late trades (spill replay, backfill) land in the right day. `missing_trade_ids` counts holes in the contiguous trade-id span. |
| `dim_symbol` | **SCD2** product dimension from a dbt snapshot of `ref.coinbase_products` (check strategy on status, tick/lot size, trading flags; products that vanish from the API are closed out). |
| `rpt_bar_reconciliation` | Streaming `gold.bars_1m` vs a batch recompute from silver for every settled minute: `match` / `count_mismatch` (late trades) / `value_mismatch` / `missing_in_stream` / `missing_in_batch`. Phase 4's stream-vs-batch reconciliation, run continuously. |

Data quality tests:
- unique keys on every table
- OHLC consistency (`low ≤ open/close ≤ high`, VWAP in range)
- **price outliers** (>20% from the daily VWAP)
- one current version per symbol
- trade-id gaps (warn)
- stream/batch value mismatches (error)
- bars missing from the stream (warn)
- late trades over 1% (error)
- source freshness

`tests/test_dbt.py` runs real `dbt build`/`snapshot`/`test` against the pytest Spark session in CI. It covers correct bars, SCD2 versioning across a status/tick change and a delisting, late trades picked up by the incremental lookback, and an outlier failing the build.

## Orchestration (Dagster)

`src/marketlake/orchestration/definitions.py`. The streaming job runs continuously outside Dagster; `silver/trades` and `gold/bars_1m` are declared as external assets so lineage and checks attach to them.

| job | schedule | what |
|---|---|---|
| `refresh_reference` | daily 00:15 | `ref/coinbase_products` |
| `build_dbt` | every 30 min | all dbt models + snapshot (dagster-dbt); the 14 single-model dbt tests appear as asset checks |
| `check_streaming` | every 10 min | `silver_trades_fresh` (newest ingest < 10 min old, warn) and `silver_reject_rate` (quarantined share of the last 24h ≤ 1%) |
| `compact_and_expire` | hourly | `maintenance` compact + expire on every table |
| `remove_orphans` | daily 03:30 | orphan-file cleanup |

`dagster dev` parses the dbt project on startup. Outside it (e.g. `dagster job execute`, a deployment), run `uv run python -m marketlake.transform parse` first so `dbt/target/manifest.json` exists.

## Dashboards (Trino + Grafana)

Trino reads the same Iceberg REST catalog and S3 warehouse as Spark and dbt (`infra/trino/catalog/lake.properties`), so every layer is queryable over SQL with no export step. Grafana queries Trino through the official [`trino-datasource`](https://github.com/trinodb/grafana-trino) plugin, installed before the server starts. The data source and dashboards are provisioned from `infra/grafana/`, so `docker compose up` brings them up configured. Both dashboards refresh every 10s.

| dashboard | panels |
|---|---|
| **Market** (home) | last price, VWAP, trade count, taker buy share; 1-minute candlesticks with volume from `gold.bars_1m`; trades/s by taker side; close vs VWAP. `$symbol` picker. |
| **Pipeline health** | seconds since last trade, exchange→ingest latency (p50/p99), quarantined records with raw payloads, stream-vs-batch mismatches and per-status counts (`rpt_bar_reconciliation`), trade-id gaps per day (`fct_bars_1d`), Iceberg data files per table (`$files`). |


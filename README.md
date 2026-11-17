# marketlake

A streaming lakehouse for crypto market data: exchange websockets → Kafka → stream processing → Iceberg → dbt → SQL.

## Architecture (target)

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
   → exactly-once proof, REST backfill, compaction,
     batch-vs-stream reconciliation                         [phase 4]
```

## Quickstart

Requires Docker, [uv](https://docs.astral.sh/uv/), and Java 17+ (for Spark).

```bash
docker compose up -d                              # Redpanda, SeaweedFS, Postgres, Iceberg REST
uv sync
uv run python -m marketlake.ingest.coinbase       # websocket → trades.raw
uv run python -m marketlake.sink.bronze           # trades.raw → data/bronze/ (Parquet)
uv run python -m marketlake.spark.stream_trades   # trades.raw → silver.trades, gold.bars_1m
uv run python -m marketlake.ref.coinbase_products # product metadata → ref.coinbase_products
uv run python -m marketlake.transform build       # dbt: snapshot, models, tests
uv run dagster dev                                # orchestration UI on :3000 (turn schedules on there)
uv run pytest
```

| service | port | |
|---|---|---|
| Redpanda (Kafka API) | 19092 | |
| Redpanda Console | 8080 | browse topics |
| Iceberg REST catalog | 8181 | |
| SeaweedFS S3 | 8333 | bucket `warehouse` |

## Design decisions

- **Partition key = `exchange:symbol`.** Keeps each symbol's trades in order within one partition, which gap detection and stateful windows downstream rely on.
- **Idempotent producer (`enable.idempotence`, `acks=all`).** Producer retries can't duplicate records in the log.
- **No silent loss when Kafka is down.** Records that fail delivery (timeout, full local queue, still queued at shutdown) go to an fsynced local journal (`data/spill/`). They are replayed once the broker accepts writes, and the journal batch is deleted only after every record is acknowledged. Replayed records never re-spill, so the journal can't grow on retries during an outage.
- **Prices as decimal strings.** No float round-trips at ingest; typing happens in silver.
- **Taker side, not maker side.** Coinbase reports the maker side. We store the aggressor side, the convention for order-flow metrics.
- **Gap detection on `trade_id`.** Coinbase trade ids are contiguous per product. The detector's state survives reconnects, so a disconnect window shows up as a gap event on `ingest.gaps`, which the phase 4 backfiller consumes. It also survives restarts: the ingestor saves the highest *durable* trade id per symbol (acked by Kafka or in the spill journal) to `data/state/` every 5s and on shutdown, and seeds the detector from it. Saving durable ids rather than seen ids matters: trades still queued in the producer when the process is killed fall inside the next run's gap and get backfilled ([#8](../../issues/8)).
- **Bronze never rejects a record.** Anything that fails parsing or validation (including partition-path values like `exchange="../.."`) is written to `exchange=_unparsed/` with its raw value and parse error. A poison pill can't stall the sink, and nothing is lost.
- **Bronze sink is at-least-once by design.** Files are fsynced and atomically renamed *before* offsets are committed. File names encode the Kafka partition and offset range (`p1-733-1255.parquet`), so a replay is auditable. Deduplication on `(exchange, symbol, trade_id)` happens in silver.

### Phase 2: stream processing

- **Silver writes are idempotent (`MERGE ... WHEN NOT MATCHED INSERT`).** Spark commits the Iceberg snapshot and then the checkpoint. A crash between the two replays the micro-batch, and the MERGE inserts nothing the second time. The result is exactly-once table contents on top of at-least-once delivery. The MERGE bounds are rendered inside Spark; collecting them as Python datetimes once shifted them by the local UTC offset and broke this ([#1](../../issues/1)). The MERGE condition also bounds target `event_ts` to the batch's min/max, so Iceberg prunes to the touched day partitions instead of scanning the whole table.
- **Silver has no watermark; gold does.** Silver accepts every trade, however late. Gold bars use a 30s event-time watermark in append mode, so each bar is written once, after its window closes. Trades later than the watermark are dropped from the *streaming* bars and counted per batch (`late_dropped`). Phase 4 reconciles the bars against a batch recompute from silver.
- **Dedupe in both layers.** Silver dedupes on the MERGE key. Gold uses `dropDuplicatesWithinWatermark`, which keeps dedupe state bounded by the watermark instead of growing forever. The same `bars_1m()` function runs on batch DataFrames (falling back to a full `dropDuplicates`), so stream and batch share one definition of a bar.
- **Open/close by `trade_id`, not arrival order.** `min_by(price, trade_id)` / `max_by(price, trade_id)` stay correct when events arrive out of order inside a window.
- **Decimal precision is deliberate.** `decimal(28,10) * decimal(28,10)` overflows 38 digits, and Spark silently cuts the result scale to 6. The notional is computed from `decimal(20,10)` operands, which gives a `decimal(38,17)` product.
- **`try_cast` on parse.** Spark 4 defaults to ANSI mode, where one malformed price throws and fails the query. A unit test caught this.
- **Rejects are quarantined, not dropped.** Each record is tagged with a `reject_reason` covering every NOT NULL silver column. A single null in a required Iceberg column fails the whole write, so a partial check is a crash waiting to happen. Rejects go to `quarantine.trades` with their raw bytes, merged on Kafka partition/offset so replays don't duplicate them, and counts are logged per batch.

### Table maintenance

`uv run python -m marketlake.spark.maintenance {compact|expire|orphans|all}` runs over silver, gold and quarantine, and is safe while the stream is writing.

- **compact:** `rewrite_data_files` (bin-pack to 128 MB, `partial-progress` so a conflict with a streaming commit only retries one file group) + `rewrite_manifests`.
- **expire:** snapshots older than 24h, but **always keeps the last 100**. The Iceberg streaming sink makes gold replays idempotent by finding its last epoch id in snapshot history. If compaction commits after the stream and expiry keeps only that snapshot, the marker is gone and a replayed epoch is appended twice. A test reproduces this.
- **orphans:** files older than 3 days that no snapshot references (younger files may belong to an in-flight commit). Uses `prefix_listing` so it lists through Iceberg's S3FileIO; the default Hadoop listing has no `s3://` filesystem.

### Phase 3: dbt

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

### Orchestration (Dagster)

`src/marketlake/orchestration/definitions.py`. The streaming job runs continuously outside Dagster; `silver/trades` and `gold/bars_1m` are declared as external assets so lineage and checks attach to them.

| job | schedule | what |
|---|---|---|
| `refresh_reference` | daily 00:15 | `ref/coinbase_products` |
| `build_dbt` | every 30 min | all dbt models + snapshot (dagster-dbt); the 14 single-model dbt tests appear as asset checks |
| `check_streaming` | every 10 min | `silver_trades_fresh` (newest ingest < 10 min old, warn) and `silver_reject_rate` (quarantined share of the last 24h ≤ 1%) |
| `compact_and_expire` | hourly | `maintenance` compact + expire on every table |
| `remove_orphans` | daily 03:30 | orphan-file cleanup |

`dagster dev` parses the dbt project on startup. Outside it (e.g. `dagster job execute`, a deployment), run `uv run python -m marketlake.transform parse` first so `dbt/target/manifest.json` exists.

## Findings

Problems that came up while running the pipeline live:

1. **SQLite can't back a catalog with concurrent writers.** The Iceberg REST fixture uses SQLite. Once two streaming queries committed at the same time, commits failed with `SQLITE_BUSY` (surfacing in Spark as `CommitStateUnknownException`). Fix: build the fixture image with the Postgres JDBC driver and point the catalog at Postgres (`infra/iceberg-rest/Dockerfile`).
2. **Overlapping ingestors duplicate data, and the pipeline absorbs it.** Two ingestor processes ran at once for about 75s by accident. Kafka ended up with 10,376 records but only 9,037 distinct trades. Silver held exactly 9,037 rows with 0 missing, and all 21 gold bars matched a batch recompute from silver (count, volume, close), including the overlap windows.
3. **Small files came from the write path, not just batch size.** Every silver commit wrote exactly 8 files, even for 16 rows. Spark disables AQE for streaming micro-batches, so the MERGE's sort-order range shuffle wrote one file per shuffle partition (8). At a 10s trigger that's ~69k files/day. Re-enabling AQE for the MERGE (a normal batch query inside `foreachBatch`) plus `write.spark.advisory-partition-size-bytes` brought it to **1 file per commit** live. Compaction (`maintenance compact`) handles the rest: 177 → 18 files, 21 → 1 manifests, run while the stream was writing. Query time on the same data: 159 ms at 41 files vs 362 ms at 194 files.
4. **Gap state didn't survive process restarts.** The gap detector caught holes across websocket reconnects, but a fresh process started with empty state, so downtime between runs went undetected. The ingestor now persists per-symbol durable trade-id marks and seeds the detector from them, so a restart reports the downtime as a gap ([#8](../../issues/8)).
5. **Well-formed garbage passes every structural check.** A trade priced at 1.00 (BTC ~65,000) is valid JSON, has every field, and is internally consistent as a bar (`low ≤ close ≤ high`). It reached silver and set BTC's daily low and close. Only a semantic check (price vs daily VWAP) caught it, so that test now exists.
6. **Rolling back a checkpoint's offsets silently drops gold bars.** The Iceberg streaming sink skips any epoch ≤ the last one it committed for that query id. After a forced replay re-planned later epochs under reused ids, bars were skipped without any error. The reconciliation report surfaced 8 missing minutes. A fresh checkpoint (new query id) is safe; editing one in place is not.
7. **Spark's progress bar ate a dbt event.** dagster-dbt parses dbt's stdout as a stream of JSON events, and Spark's console progress bar writes to the same stdout. A `[Stage 36: …]\r` line glued to `fct_bars_1d`'s success event made it unparseable, so Dagster silently recorded no materialization for that model (the run still succeeded). It's intermittent, since it only hits events emitted while a stage is running. Fixed with `spark.ui.showConsoleProgress=false` in `spark_conf()`.

## Fault-injection testing (2026-11-15)

| test | result | issue |
|---|---|---|
| Clean clone → `uv sync` → `pytest` | ✅ 8/8 pass | |
| Snapshot (SCD2) on the second run | ❌ → ✅ dbt-spark's Iceberg path created its staging view by bare name in the default catalog (`lake`) → `VIEW_NOT_FOUND`; fixed with a temporary-view macro override | |
| Replay a committed **silver** batch (delete checkpoint commit) | ❌ → ✅ +328 duplicate rows from MERGE bounds shifted by local TZ; after fix a 2,549-row replay adds 0 | [#1](../../issues/1) |
| Replay a committed **gold** epoch that emitted bars | ✅ skipped via Iceberg `epochId` in snapshot summary; 0 duplicate bars | |
| `kill -9` stream job ×3 during live ingest, then drain | ✅ silver: 0 missing of 17,886 trades; gold: 33/33 bars match batch recompute (OHLC, VWAP, volume, count) | |
| Malformed JSON / non-numeric price into `trades.raw` | ✅ stream skips them; ❌ → ✅ bronze sink crash-looped, now quarantines to `exchange=_unparsed` with raw value + error; ⚠️ → ✅ stream drops were uncounted, now go to `quarantine.trades` | [#3](../../issues/3), [#7](../../issues/7) |
| Valid JSON missing `exchange` | ❌ → ✅ failed every silver write (`NOT_NULL_ASSERT_VIOLATION`) and would fail again after each restart; now quarantined | [#7](../../issues/7) |
| Trade 2h older than watermark | ✅ kept in silver, excluded from gold, `late_dropped=1` | |
| Kafka down 20s (8s delivery timeout) | ❌ → ✅ 804/1,678 trades silently lost; now spilled to a local journal and replayed: 848 spilled, 848 replayed, 3,001/3,001 in Kafka, 0 duplicates | [#2](../../issues/2) |
| Websocket handshake rejected | ❌ → ✅ ingestor exited (`InvalidStatus`); now backs off (honoring `Retry-After`) and skips malformed messages, which surface as gaps | [#4](../../issues/4) |
| `rpk` vs Python producer, same key | ⚠️ partition 5 vs 1 (murmur2 vs CRC32) | [#5](../../issues/5) |
| Gold snapshot history | ⚠️ → ✅ empty snapshot every 10s trigger; now expired by `maintenance expire` | [#6](../../issues/6) |
| `maintenance all` while ingest + stream are writing | ✅ no conflicts; stream kept committing; silver 28,585/28,585 distinct, gold 60/60 | |
| Replay a gold epoch after compaction + expiry | ✅ idempotent with `retain_last=100`; ❌ with `retain_last=1` the epoch marker is expired and the replay appends duplicates (35 rows vs 30), now a regression test | |

## Results log

| date | run | result |
|---|---|---|
| 2026-11-15 | 45s live, BTC/ETH/SOL-USD | 1,711 trades (~43/s); 0 dupes, 0 id holes in bronze; committed offsets == rows written |
| 2026-11-15 | Spark streaming, 10s trigger, local[4] | micro-batches ~1.0s for 250–650 rows; restart from checkpoint resumed at batch 19 with no reprocessing |
| 2026-11-15 | Kafka vs silver vs gold | 10,376 Kafka records → 9,037 distinct = 9,037 silver rows; 21/21 gold bars match batch recompute |

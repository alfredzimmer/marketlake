# Worklog

Verification runs and numbers that don't belong in the README.

## 2026-11-17: ingest outage on the dashboards

An ingest outage is visible on both dashboards: on 2026-11-17 the Coinbase websocket hit a keepalive timeout at 16:05 UTC and took 48s to reconnect. The candlestick chart is missing that minute, the per-minute charts break, and the gap detector reported the 7,334 missing trade ids on `ingest.gaps`.

## Findings (2026-11-15 to 2026-11-17)

Problems that came up while running the pipeline live:

1. **SQLite can't back a catalog with concurrent writers.** The Iceberg REST fixture uses SQLite. Once two streaming queries committed at the same time, commits failed with `SQLITE_BUSY` (surfacing in Spark as `CommitStateUnknownException`). Fix: build the fixture image with the Postgres JDBC driver and point the catalog at Postgres (`infra/iceberg-rest/Dockerfile`).
2. **Overlapping ingestors duplicate data, and the pipeline absorbs it.** Two ingestor processes ran at once for about 75s by accident. Kafka ended up with 10,376 records but only 9,037 distinct trades. Silver held exactly 9,037 rows with 0 missing, and all 21 gold bars matched a batch recompute from silver (count, volume, close), including the overlap windows.
3. **Small files came from the write path, not just batch size.** Every silver commit wrote exactly 8 files, even for 16 rows. Spark disables AQE for streaming micro-batches, so the MERGE's sort-order range shuffle wrote one file per shuffle partition (8). At a 10s trigger that's ~69k files/day. Re-enabling AQE for the MERGE (a normal batch query inside `foreachBatch`) plus `write.spark.advisory-partition-size-bytes` brought it to **1 file per commit** live. Compaction (`maintenance compact`) handles the rest: 177 → 18 files, 21 → 1 manifests, run while the stream was writing. Query time on the same data: 159 ms at 41 files vs 362 ms at 194 files.
4. **Gap state didn't survive process restarts.** The gap detector caught holes across websocket reconnects, but a fresh process started with empty state, so downtime between runs went undetected. The ingestor now persists per-symbol durable trade-id marks and seeds the detector from them, so a restart reports the downtime as a gap ([#8](../../../issues/8)).
5. **Well-formed garbage passes every structural check.** A trade priced at 1.00 (BTC ~65,000) is valid JSON, has every field, and is internally consistent as a bar (`low ≤ close ≤ high`). It reached silver and set BTC's daily low and close. Only a semantic check (price vs daily VWAP) caught it, so that test now exists.
6. **Rolling back a checkpoint's offsets silently drops gold bars.** The Iceberg streaming sink skips any epoch ≤ the last one it committed for that query id. After a forced replay re-planned later epochs under reused ids, bars were skipped without any error. The reconciliation report surfaced 8 missing minutes. A fresh checkpoint (new query id) is safe; editing one in place is not.
7. **Spark's progress bar ate a dbt event.** dagster-dbt parses dbt's stdout as a stream of JSON events, and Spark's console progress bar writes to the same stdout. A `[Stage 36: …]\r` line glued to `fct_bars_1d`'s success event made it unparseable, so Dagster silently recorded no materialization for that model (the run still succeeded). It's intermittent, since it only hits events emitted while a stage is running. Fixed with `spark.ui.showConsoleProgress=false` in `spark_conf()`.
8. **`query.stop()` mid-batch ends in a `StackOverflowError`.** `stop()` interrupts the stream thread. When a micro-batch is running (the silver `foreachBatch`, or the Kafka consumer closing), the interrupted job fails with a deep regex-matching stack overflow. Live with a 2s trigger, 3 of 6 stops hit it. No data was affected, since the batch replays idempotently, but the noise could hide a real error. The stream now waits for the in-flight batch to finish before stopping: 0 errors in 8 stops ([#19](../../../issues/19)).
9. **Docker Desktop's port proxy, not the object store, broke large scans.** From the host, repeated scans of a 6,000-file table failed on the 5th run with S3 `Connection reset` after 6 SDK retries. SeaweedFS logged nothing. The same scans from a container on the compose network passed 10/10 and ran 2x faster (2.2s vs 4.6s), while a host run straight afterwards failed again. At 17,280 files: 5/5 in-network, 1/3 from the host. The `spark` compose service (`profiles: [spark]`) runs jobs in-network with the same `uv.lock` dependencies; `marketlake.spark.bench_files` reproduces the comparison ([#18](../../../issues/18)).

## 2026-11-15: fault-injection testing

| test | result | issue |
|---|---|---|
| Clean clone → `uv sync` → `pytest` | ✅ 8/8 pass | |
| Snapshot (SCD2) on the second run | ❌ → ✅ dbt-spark's Iceberg path created its staging view by bare name in the default catalog (`lake`) → `VIEW_NOT_FOUND`; fixed with a temporary-view macro override | |
| Replay a committed **silver** batch (delete checkpoint commit) | ❌ → ✅ +328 duplicate rows from MERGE bounds shifted by local TZ; after fix a 2,549-row replay adds 0 | [#1](../../../issues/1) |
| Replay a committed **gold** epoch that emitted bars | ✅ skipped via Iceberg `epochId` in snapshot summary; 0 duplicate bars | |
| `kill -9` stream job ×3 during live ingest, then drain | ✅ silver: 0 missing of 17,886 trades; gold: 33/33 bars match batch recompute (OHLC, VWAP, volume, count) | |
| Malformed JSON / non-numeric price into `trades.raw` | ✅ stream skips them; ❌ → ✅ bronze sink crash-looped, now quarantines to `exchange=_unparsed` with raw value + error; ⚠️ → ✅ stream drops were uncounted, now go to `quarantine.trades` | [#3](../../../issues/3), [#7](../../../issues/7) |
| Valid JSON missing `exchange` | ❌ → ✅ failed every silver write (`NOT_NULL_ASSERT_VIOLATION`) and would fail again after each restart; now quarantined | [#7](../../../issues/7) |
| Trade 2h older than watermark | ✅ kept in silver, excluded from gold, `late_dropped=1` | |
| Kafka down 20s (8s delivery timeout) | ❌ → ✅ 804/1,678 trades silently lost; now spilled to a local journal and replayed: 848 spilled, 848 replayed, 3,001/3,001 in Kafka, 0 duplicates | [#2](../../../issues/2) |
| Websocket handshake rejected | ❌ → ✅ ingestor exited (`InvalidStatus`); now backs off (honoring `Retry-After`) and skips malformed messages, which surface as gaps | [#4](../../../issues/4) |
| `rpk` vs Python producer, same key | ⚠️ → ✅ partition 5 vs 1 (murmur2 vs CRC32); the ingestor now uses murmur2, checked against Java's algorithm on librdkafka's mock cluster | [#5](../../../issues/5) |
| Gold snapshot history | ⚠️ → ✅ empty snapshot every 10s trigger; now expired by `maintenance expire` | [#6](../../../issues/6) |
| `maintenance all` while ingest + stream are writing | ✅ no conflicts; stream kept committing; silver 28,585/28,585 distinct, gold 60/60 | |
| Replay a gold epoch after compaction + expiry | ✅ idempotent with `retain_last=100`; ❌ with `retain_last=1` the epoch marker is expired and the replay appends duplicates (35 rows vs 30), now a regression test | |

## Results log (2026-11-15)

| date | run | result |
|---|---|---|
| 2026-11-15 | 45s live, BTC/ETH/SOL-USD | 1,711 trades (~43/s); 0 dupes, 0 id holes in bronze; committed offsets == rows written |
| 2026-11-15 | Spark streaming, 10s trigger, local[4] | micro-batches ~1.0s for 250–650 rows; restart from checkpoint resumed at batch 19 with no reprocessing |
| 2026-11-15 | Kafka vs silver vs gold | 10,376 Kafka records → 9,037 distinct = 9,037 silver rows; 21/21 gold bars match batch recompute |

## 2026-11-19 — REST backfill (`marketlake.ingest.backfill`)

- Live REST: a 7,334-id BTC-USD range (the size of the 2026-11-17 16:05 outage) fetched in 8 pages / 1.6s, contiguous, 0 duplicates.
- End to end on the three newest `ingest.gaps` events (ETH 319, BTC 2,912, SOL 182 ids): all `complete` on `ingest.backfills`, group lag 0, all 3,413 ids in `silver.trades`. Gold dropped them as late (expected; the batch layers recompute from silver).
- Replay: rewound the group and re-ran the same gaps. Silver stayed at 3,413 rows; 0 duplicate `(symbol, trade_id)` across the table.
- Not yet run: the 390 older gap events (~3.2M ids, mostly 2026-11-17/19 downtime). At 5 req/s that's ~11 min of paging.

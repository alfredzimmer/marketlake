# Worklog

Verification runs and numbers that don't belong in the README.

## 2026-11-19 — REST backfill (`marketlake.ingest.backfill`)

- Live REST: a 7,334-id BTC-USD range (the size of the 2026-11-17 16:05 outage) fetched in 8 pages / 1.6s, contiguous, 0 duplicates.
- End to end on the three newest `ingest.gaps` events (ETH 319, BTC 2,912, SOL 182 ids): all `complete` on `ingest.backfills`, group lag 0, all 3,413 ids in `silver.trades`. Gold dropped them as late (expected; the batch layers recompute from silver).
- Replay: rewound the group and re-ran the same gaps. Silver stayed at 3,413 rows; 0 duplicate `(symbol, trade_id)` across the table.
- Not yet run: the 390 older gap events (~3.2M ids, mostly 2026-11-17/19 downtime). At 5 req/s that's ~11 min of paging.

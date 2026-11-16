-- Warn, don't fail: gaps are real (ingestor downtime, #8) and are recovered by
-- backfill, but they must be visible.
{{ config(severity='warn') }}
select exchange, symbol, trade_date, missing_trade_ids
from {{ ref('fct_bars_1d') }}
where missing_trade_ids > 0

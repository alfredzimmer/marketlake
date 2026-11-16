-- Streaming gold bars vs a batch recompute from silver, per closed minute.
-- Differences are expected only for trades that arrived after the stream's
-- watermark (counted as late_dropped by the job) and for windows the stream
-- never saw (it wasn't running).
with settled as (
    select max(event_ts) - interval {{ var('reconcile_settle_minutes') }} minutes as cutoff
    from {{ ref('stg_trades') }}
),

stream as (
    select * from {{ source('gold', 'bars_1m') }}
    where window_start < (select cutoff from settled)
),

batch as (
    select * from {{ ref('stg_bars_1m_batch') }}
    where window_start < (select cutoff from settled)
)

select
    coalesce(s.exchange, b.exchange) as exchange,
    coalesce(s.symbol, b.symbol) as symbol,
    coalesce(s.window_start, b.window_start) as window_start,
    s.trade_count as stream_trade_count,
    b.trade_count as batch_trade_count,
    b.trade_count - s.trade_count as late_trades,
    case
        when s.window_start is null then 'missing_in_stream'
        when b.window_start is null then 'missing_in_batch'
        when s.trade_count <> b.trade_count then 'count_mismatch'
        when s.open <> b.open or s.high <> b.high or s.low <> b.low or s.close <> b.close
          or s.volume <> b.volume then 'value_mismatch'
        else 'match'
    end as status
from stream s
full outer join batch b
    on s.exchange = b.exchange and s.symbol = b.symbol and s.window_start = b.window_start

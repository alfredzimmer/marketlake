-- Batch recompute of 1-minute bars from silver, the same definition the stream
-- uses (open/close by trade_id, notional from decimal(20,10) operands).
select
    exchange,
    symbol,
    date_trunc('minute', event_ts) as window_start,
    min_by(price, trade_id) as open,
    max(price) as high,
    min(price) as low,
    max_by(price, trade_id) as close,
    cast(sum(size) as decimal(38,10)) as volume,
    cast(sum(cast(price as decimal(20,10)) * cast(size as decimal(20,10))) as decimal(38,10)) as notional,
    count(*) as trade_count
from {{ ref('stg_trades') }}
group by 1, 2, 3

select
    exchange,
    symbol,
    trade_id,
    price,
    size,
    taker_side,
    event_ts,
    to_date(event_ts) as trade_date,
    ingest_ts
from {{ source('silver', 'trades') }}

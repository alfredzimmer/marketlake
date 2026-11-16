{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['exchange', 'symbol', 'trade_date'],
) }}

with trades as (
    select * from {{ ref('stg_trades') }}
    {% if is_incremental() %}
    -- Recompute trailing days, not just new ones: late trades land in old days.
    where trade_date >= (
        select date_sub(max(trade_date), {{ var('late_data_lookback_days') }}) from {{ this }}
    )
    {% endif %}
),

daily as (
    select
        exchange,
        symbol,
        trade_date,
        min_by(price, trade_id) as open,
        max(price) as high,
        min(price) as low,
        max_by(price, trade_id) as close,
        cast(sum(size) as decimal(38,10)) as volume,
        cast(sum(cast(price as decimal(20,10)) * cast(size as decimal(20,10))) as decimal(38,10)) as notional,
        count(*) as trade_count,
        sum(case when taker_side = 'buy' then size else 0 end) as buy_volume,
        min(trade_id) as first_trade_id,
        max(trade_id) as last_trade_id
    from trades
    group by 1, 2, 3
)

select
    exchange,
    symbol,
    trade_date,
    open,
    high,
    low,
    close,
    volume,
    notional,
    cast(notional / volume as decimal(28,10)) as vwap,
    trade_count,
    cast(buy_volume as decimal(38,10)) as buy_volume,
    first_trade_id,
    last_trade_id,
    -- Coinbase trade ids are contiguous per product: anything short of the id
    -- span was never ingested (downtime, see #8).
    (last_trade_id - first_trade_id + 1) - trade_count as missing_trade_ids
from daily

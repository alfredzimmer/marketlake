-- SCD2 product dimension: one row per version of a product's trading
-- attributes, built from the snapshot of ref.coinbase_products.
select
    id as symbol,
    base_currency,
    quote_currency,
    cast(quote_increment as decimal(28,10)) as tick_size,
    cast(base_increment as decimal(28,10)) as lot_size,
    cast(min_market_funds as decimal(28,10)) as min_market_funds,
    status,
    trading_disabled,
    post_only,
    limit_only,
    cancel_only,
    auction_mode,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('snap_coinbase_products') }}

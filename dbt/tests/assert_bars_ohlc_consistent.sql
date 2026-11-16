-- low <= open/close <= high and vwap within [low, high], for streaming and daily bars.
select 'gold.bars_1m' as source, exchange, symbol, cast(window_start as string) as bar
from {{ source('gold', 'bars_1m') }}
where not (low <= open and open <= high and low <= close and close <= high
           and vwap between low and high and volume > 0)
union all
select 'fct_bars_1d', exchange, symbol, cast(trade_date as string)
from {{ ref('fct_bars_1d') }}
where not (low <= open and open <= high and low <= close and close <= high
           and vwap between low and high and volume > 0)

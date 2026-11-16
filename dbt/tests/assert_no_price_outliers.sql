-- Trades priced more than 20% away from their symbol's daily VWAP. Catches
-- fat-finger/garbage prices that are otherwise well-formed (and so pass
-- parsing, quarantine and OHLC consistency checks).
select t.exchange, t.symbol, t.trade_id, t.event_ts, t.price, d.vwap
from {{ ref('stg_trades') }} t
join {{ ref('fct_bars_1d') }} d
  on t.exchange = d.exchange and t.symbol = d.symbol and t.trade_date = d.trade_date
where abs(t.price / d.vwap - 1) > 0.2

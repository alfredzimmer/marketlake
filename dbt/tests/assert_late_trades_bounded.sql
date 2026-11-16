-- Late trades dropped from streaming bars: warn if any, fail above 1% of trades.
{{ config(severity='error', warn_if='>0', error_if='>0') }}
select sum(late_trades) as late, sum(batch_trade_count) as total
from {{ ref('rpt_bar_reconciliation') }}
where status = 'count_mismatch'
having sum(late_trades) > 0.01 * (select sum(batch_trade_count) from {{ ref('rpt_bar_reconciliation') }})

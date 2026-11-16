select symbol, count(*) as current_versions
from {{ ref('dim_symbol') }}
where is_current
group by symbol
having count(*) > 1

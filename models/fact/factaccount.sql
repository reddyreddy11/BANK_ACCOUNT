with intermediate as (
    select *
    from {{ ref('intermediate_account') }}
),
window_calc as (
    select *,
           lag(balance) over (partition by account_id order by load_ts) as opening_balance
    from intermediate
),
factaccount as (
    select *
    from window_calc
)
select *
from factaccount
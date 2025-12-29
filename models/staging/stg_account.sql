{{ config(materialized='table') }}

select
Account_id,
    upper(trim(account_name)) as account_name,
    
    case
        when lower(account_type) in ('saving','savings') then 'SAVING'
        when lower(account_type) in ('current','checking') then 'CURRENT'
        else 'OTHER'
    end as account_type,
    
    balance,
    
    case
        when balance < 5000 then 'SMALL'
        when balance between 5000 and 50000 then 'MID'
        else 'LARGE'
    end as balance_end,
    
    case
        when balance > 0 then 'ACTIVE'
        else 'DORMANT'
    end as account_status,
    
    case
        when balance = 0 then 'Y'
        else 'N'
    end as zero_balance,
    
    current_timestamp() as load_ts   -- <--- Make sure alias exists

from {{ source('putty','ACCOUNT') }}

where balance > 0


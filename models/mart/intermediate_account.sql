{{
  config(
    materialized='incremental',
    unique_key='Account_id',
    incremental_strategy='append'
  )
}}

select
Account_id,
    upper(trim(account_name)) as account_name,
    account_type,
    balance,
    balance_end,
    account_status,
    zero_balance,
    load_ts
from {{ ref('stg_account') }}
from {{ ref('stg_account') }}
{% if is_incremental() %}
where load_ts >= coalesce((select max(load_ts) from {{ this }}), '1900-01-01')
{% endif %}




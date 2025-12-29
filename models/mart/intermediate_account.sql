{{
  config(
    materialized='incremental',
    unique_key='ACCOUNT_ID',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
  )
}}

select
    ACCOUNT_ID,
    upper(trim(ACCOUNT_NAME)) as account_name,
    ACCOUNT_TYPE,
    BALANCE,
    BALANCE_END,
    ACCOUNT_STATUS,
    ZERO_BALANCE,
    LOAD_TS
from {{ ref('stg_account') }}
{% if is_incremental() %}
where LOAD_TS > (select max(LOAD_TS) from {{ this }})
{% endif %}




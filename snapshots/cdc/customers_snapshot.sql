{% snapshot customers_snapshot %}

{{
    config(
        target_schema='snapshot',
        unique_key='customer_id',
        strategy='check',
        check_cols=[
            'email',
            'loyalty_points',
            'account_balance',
            'status'
        ]
    )
}}
SELECT
    customer_id,
    customer_code,
    first_name,
    last_name,
    full_name,
    email,
    phone_number,
    date_of_birth,
    gender,
    address_line1,
    address_line2,
    city,
    state,
    country,
    postal_code,
    customer_type,
    loyalty_points,
    account_balance,
    status,
    is_email_verified,
    is_phone_verified,
    created_at,
    updated_at
FROM {{ source('raw', 'customers') }}

{% endsnapshot %}
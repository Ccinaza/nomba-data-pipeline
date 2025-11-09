{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='delete+insert',
    engine='MergeTree',
    order_by=['transaction_id', 'updated_at'],
    settings={'allow_nullable_key': 1}
) }}


select
    txn_id as transaction_id,
    plan_id,
    amount,
    currency,
    side,
    rate,
    toTimeZone(txn_timestamp, 'Africa/Lagos') as created_at,
    toTimeZone(updated_at, 'Africa/Lagos') as updated_at,
    deleted_at
from {{ source('nomba', 'raw_savings_transactions') }}

-- This block only runs when table exists AND it's not a full refresh
{% if is_incremental() %}
  -- Only include rows where updated_at is newer than the max in the target table
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
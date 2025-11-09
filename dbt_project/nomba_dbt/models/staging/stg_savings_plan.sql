{{ config(
    materialized='incremental',
    unique_key='plan_id',
    incremental_strategy='delete+insert',
    engine='MergeTree',
    order_by=['plan_id', 'updated_at'],
) }}

select
    plan_id,
    product_type,
    customer_uid as user_id,
    amount,
    frequency,
    start_date,
    end_date,
    toTimeZone(created_at, 'Africa/Lagos') as created_at,
    toTimeZone(updated_at, 'Africa/Lagos') as updated_at
from {{ source('nomba', 'raw_plans') }}

-- This block only runs when table exists AND it's not a full refresh
{% if is_incremental() %}
  -- Only include rows where updated_at is newer than the max in the target table
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}

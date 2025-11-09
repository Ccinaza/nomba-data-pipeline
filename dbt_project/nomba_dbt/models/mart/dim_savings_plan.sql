{{ config(
    materialized='incremental',
    unique_key='plan_id',
    incremental_strategy='delete+insert',
    engine='MergeTree',
    order_by=['plan_id', 'start_date', 'updated_at'],
    settings={'allow_nullable_key': 1}
) }}

select
    p.plan_id,
    p.user_id as user_id,
    u.full_name as customer_name,
    u.state as customer_state,
    u.occupation as user_occupation,
    p.amount as plan_amount,
    p.product_type,
    p.frequency,
    p.start_date,
    p.end_date,
    p.created_at as created_at,
    p.updated_at as updated_at
    
from {{ ref('stg_savings_plan') }} p
left join {{ ref('dim_users') }} u
    on p.user_id = u.user_id

-- This block only runs when table exists AND it's not a full refresh
{% if is_incremental() %}
  -- Only include rows where updated_at is newer than the max in the target table
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}

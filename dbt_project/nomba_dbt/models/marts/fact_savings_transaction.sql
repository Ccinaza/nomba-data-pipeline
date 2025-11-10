{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='delete+insert',
    engine='MergeTree',
    order_by=['updated_at', 'transaction_id'],
    settings={'allow_nullable_key': 1}
) }}

select
    t.transaction_id,
    t.plan_id,
    p.user_id,
    p.product_type,
    t.amount,
    t.currency,
    t.side,
    t.rate,
    t.created_at,
    t.updated_at
from {{ ref('stg_savings_transaction') }} t
left join {{ ref('dim_savings_plan') }} p
    on t.plan_id = p.plan_id

{% if is_incremental() %}
  where t.updated_at > (select max(updated_at) from {{ this }})
{% endif %}
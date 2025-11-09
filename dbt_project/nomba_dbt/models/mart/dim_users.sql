{{ config(
    materialized='table',
    engine='MergeTree',
    order_by=['user_id']
) }}

with latest_snapshot as (
    select
        user_id,
        first_name,
        last_name,
        occupation,
        state,
        snapshot_date,
        row_number() over (partition by user_id order by snapshot_date desc) as rn
    from {{ ref('stg_users') }}
)

select
    user_id,
    concat(first_name, ' ', last_name) as full_name,
    first_name,
    last_name,
    occupation,
    state,
    snapshot_date as last_updated_date
from latest_snapshot
where rn = 1
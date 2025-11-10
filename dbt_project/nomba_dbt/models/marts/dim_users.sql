{{ config(
    materialized='table',
    order_by='user_id'
) }}

select
    user_id,
    concat(first_name, ' ', last_name) as full_name,
    first_name,
    last_name,
    occupation,
    state,
    dbt_valid_from as last_updated_date
from {{ ref('users_snapshot') }}
where dbt_valid_to is null  -- Only get current records
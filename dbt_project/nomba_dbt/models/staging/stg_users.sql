{{ config(
    materialized='table', 
    engine='MergeTree',
    order_by=['snapshot_date', 'user_id']
) }}

select
    _id as user_table_id,
    _Uid as user_id,
    firstName as first_name,
    lastName as last_name,
    occupation,
    state,
    snapshot_date
from {{ source('nomba', 'raw_users') }}

-- -- This block only runs when table exists AND it's not a full refresh
-- {% if is_incremental() %}
--   -- Only include rows where updated_at is newer than the max in the target table
--   where snapshot_date >= (select max(snapshot_date) from {{ this }})
-- {% endif %}


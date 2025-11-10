{{ config(
    materialized='table', 
    engine='MergeTree',
    order_by=['user_id']
) }}

select
    _id as user_table_id,
    _Uid as user_id,
    firstName as first_name,
    lastName as last_name,
    occupation,
    state
from {{ source('nomba', 'raw_users') }}


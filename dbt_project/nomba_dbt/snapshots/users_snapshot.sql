{% snapshot users_snapshot %}
{{ config(
    target_schema='nomba',
    unique_key='user_id',
    strategy='check',
    check_cols=['state', 'occupation']
) }}

select
    user_table_id,
    user_id,
    first_name,
    last_name,
    occupation,
    state
from {{ ref('stg_users') }}

{% endsnapshot %}

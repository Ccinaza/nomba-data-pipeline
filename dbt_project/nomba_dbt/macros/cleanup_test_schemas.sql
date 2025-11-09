{% macro cleanup_test_schemas(schema_prefix, mr_id) %}
    {% for schema_type in ['staging', 'intermediate', 'mart'] %}
        {% set schema_name = schema_prefix ~ '_mr_' ~ mr_id ~ '_' ~ schema_type %}
        {% do run_query('DROP DATABASE IF EXISTS ' ~ schema_name) %}
        {{ log("Dropped schema: " ~ schema_name, info=True) }}
    {% endfor %}
{% endmacro %}
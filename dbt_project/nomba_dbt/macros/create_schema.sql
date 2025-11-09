{% macro create_schema_if_not_exists(schema_name) %}
    {% set sql %}
        CREATE DATABASE IF NOT EXISTS {{ schema_name }}
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
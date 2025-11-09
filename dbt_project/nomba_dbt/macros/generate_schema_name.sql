{% macro generate_schema_name(custom_schema_name, node) %}
    {% set profile_schema = target.schema %}

    {% if target.name in ['prod', 'test'] %}
        {% if custom_schema_name is not none %}
            {{ return(profile_schema ~ '_' ~ custom_schema_name | trim) }}
        {% else %}
            {{ return('nomba_analytics' if target.name == 'prod' else 'nomba_analytics_test') }}
        {% endif %}
    {% else %}
        {# Only read NOMBA_DBT_DEV_USER for dev target #}
        {% set user_schema = env_var('NOMBA_DBT_DEV_USER', none) %}
        {% if user_schema is none %}
            {{ exceptions.raise_compiler_error("ERROR: NOMBA_DBT_DEV_USER is not set. Run: export NOMBA_DBT_DEV_USER=<your_name>") }}
        {% endif %}
        {% set user_schema = user_schema | lower %}
        {% if custom_schema_name is not none %}
            {{ return(profile_schema ~ '_' ~ user_schema ~ '_' ~ custom_schema_name | trim) }}
        {% else %}
            {{ return(user_schema) }}
        {% endif %}
    {% endif %}
{% endmacro %}



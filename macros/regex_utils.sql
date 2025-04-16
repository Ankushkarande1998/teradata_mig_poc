{% macro get_regex_condition(column, pattern) %}
    {% if target.type == 'redshift' %}
        {{ column }} SIMILAR TO '{{ pattern }}'
    {% elif target.type == 'databricks' or target.type == 'trino' %}
        REGEXP_LIKE({{ column }}, '{{ pattern }}')
    {% endif %}
{% endmacro %}

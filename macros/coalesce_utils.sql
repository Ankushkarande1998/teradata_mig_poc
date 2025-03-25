{% macro get_coalesce(column, default_value) %}
    {% if target.type == 'redshift' %}
        COALESCE({{ column }}, {{ default_value }})
    {% elif target.type == 'databricks' %}
        NVL({{ column }}, {{ default_value }})
    {% elif target.type == 'starburst' %}
        COALESCE({{ column }}, {{ default_value }})
    {% endif %}
{% endmacro %}

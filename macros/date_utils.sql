{% macro get_date_sub(date_column, days) %}
    {% if target.type == 'redshift' %}
        DATEADD(DAY, -{{ days }}, {{ date_column }}::DATE)
    {% elif target.type == 'databricks' %}
        DATE_SUB(DATE({{ date_column }}), {{ days }})
    {% elif target.type == 'starburst' %}
        {{ date_column }} - INTERVAL '{{ days }}' DAY
    {% endif %}
{% endmacro %}

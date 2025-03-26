{% macro get_date_sub(days) %}
    {% if target.type == 'redshift' %}
        DATEADD(DAY, -{{ days }}, CURRENT_DATE)
    {% elif target.type == 'databricks' %}
        DATE_SUB(CURRENT_DATE(), {{ days }})
    {% elif target.type == 'starburst' %}
        CURRENT_DATE - INTERVAL '{{ days }}' DAY
    {% endif %}
{% endmacro %}

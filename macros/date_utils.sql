{% macro get_date_sub(days) %}
    {% if target.type == 'redshift' %}
        DATEADD(DAY, -{{ days }}, CURRENT_DATE)
    {% elif target.type == 'databricks' %}
        DATE_SUB(CURRENT_DATE(), {{ days }})
    {% elif target.type == 'trino' %}
        CURRENT_DATE - INTERVAL '{{ days }}' DAY
    {% endif %}
{% endmacro %}


{% macro convert_start_date(date) %}
    {% if target.type == 'redshift' %}
        TO_DATE({{date}}, 'YYYYMM')
    {% elif target.type == 'databricks' %}
        TO_DATE({{date}}, 'yyyyMM')
    {% elif target.type == 'trino' %}
        date_parse(CAST({{date}} AS VARCHAR), '%Y%m')
    {% else %}
        '2025-02-01'  -- Default Fallback
    {% endif %}
{% endmacro %}
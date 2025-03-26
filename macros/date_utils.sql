{% macro get_date_sub(days) %}
    {% if target.type == 'redshift' %}
        DATEADD(DAY, -{{ days }}, CURRENT_DATE)
    {% elif target.type == 'databricks' %}
        DATE_SUB(CURRENT_DATE(), {{ days }})
    {% elif target.type == 'starburst' %}
        CURRENT_DATE - INTERVAL '{{ days }}' DAY
    {% endif %}
{% endmacro %}


{% macro convert_start_date(date) %}
    {% if target.type == 'redshift' %}
        TO_DATE({{date}}, 'YYYYMM')
    {% elif target.type == 'databricks' %}
        TO_DATE({{date}}, 'yyyyMM')
    {% elif target.type == 'starburst' %}
        DATE_PARSE({{date}}, '%Y%m')
    {% else %}
        '2025-02-01'  -- Default Fallback
    {% endif %}
{% endmacro %}
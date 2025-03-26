{% macro cast_null_as_decimal(precision=15, scale=6) %}
  {% if target.type == 'databricks' %}
    CAST(NULL AS DECIMAL({{ precision }}, {{ scale }}))
  {% elif target.type in ('trino', 'presto') %}
    NULL::DECIMAL({{ precision }}, {{ scale }})
  {% else %}
    CAST(NULL AS DECIMAL({{ precision }}, {{ scale }}))
  {% endif %}
{% endmacro %}

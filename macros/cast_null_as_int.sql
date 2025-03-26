{% macro cast_null_as_int() %}
  {% if target.type == 'redshift' %}
    CAST(NULL AS INTEGER)  -- Redshift prefers INTEGER over INT
  {% elif target.type in ('trino', 'presto') %}
    NULL::INTEGER  -- Starburst (Trino/Presto) allows "::" notation
  {% else %}
    CAST(NULL AS INT)  -- Default for Databricks and other databases
  {% endif %}
{% endmacro %}

{% macro cast_null_as_varchar(size=255) %}
  {% if target.type == 'databricks' %}
    CAST(NULL AS VARCHAR({{ size }}))
  {% elif target.type == 'trino' or target.type == 'presto' %}
    CAST(NULL AS VARCHAR({{ size }}))
  {% else %}
    CAST(NULL AS VARCHAR({{ size }}))
  {% endif %}
{% endmacro %}
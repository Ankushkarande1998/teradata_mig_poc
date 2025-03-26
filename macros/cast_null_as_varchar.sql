{% macro cast_null_as_varchar(size=255) %}
  {% if target.type == 'databricks' %}
    CAST(NULL VARCHAR({{ size }}))
  {% elif target.type == 'trino' or target.type == 'presto' %}
    NULL::VARCHAR({{ size }})
  {% else %}
    CAST(NULL AS VARCHAR({{ size }}))
  {% endif %}
{% endmacro %}
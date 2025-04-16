{% macro drop_prefixed_tables(schema, prefix) %}
  {% set results = run_query("SHOW TABLES IN " ~ schema) %}
  {% if execute %}
    {% for row in results %}
      {% set table = row['tableName'] %}
      {% if table.startswith(prefix) %}
        {{ log("Dropping table: " ~ schema ~ "." ~ table, info=True) }}
        {% do run_query("DROP TABLE IF EXISTS " ~ schema ~ "." ~ table) %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}
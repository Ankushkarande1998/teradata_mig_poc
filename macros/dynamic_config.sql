{% macro set_model_config() %}
  {% if target.type == 'trino' %}
    {{ config(
        materialized='table',
        on_table_exists='replace',
        catalog='iceberg'
    ) }}
  {% elif target.type in ['databricks', 'spark'] %}
    {{ config(
        materialized='table',
        pre_hook="DROP TABLE IF EXISTS {{ this }}"
    ) }}
  {% elif target.type == 'redshift' %}
    {{ config(
        materialized='table',
        pre_hook="DROP TABLE IF EXISTS {{ this }}"
    ) }}
  {% else %}
    {{ config(
        materialized='table'
    ) }}
  {% endif %}
{% endmacro %}

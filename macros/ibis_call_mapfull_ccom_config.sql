{% macro ibis_call_mapfull_ccom_config() %}
  {{ 
    config(
      materialized='incremental',
      incremental_strategy='merge',
      table_format='iceberg',
      unique_key=['IN_CDR_FILE_ID','IN_CDR_SERIAL_NR','OUT_CDR_FILE_ID', 'OUT_CDR_SERIAL_NR'],
      post_hook=" {{ drop_tables_by_prefix('q1_')}}"
    ) 
  }}
{% endmacro %}

{% macro ccom_nbrs_bought_with_usage_config() %}
  {{ 
    config(
      materialized='incremental',
      incremental_strategy='merge',
      table_format='iceberg',
      unique_key=['CUST_OPER_CD', 'CUST_CTRY_CD', 'MONTHSTART', 'PRODUCT', 'ACCESS_NUMBER'],
      post_hook=" {{ drop_tables_by_prefix('q2_')}}"
    ) 
  }}
{% endmacro %}

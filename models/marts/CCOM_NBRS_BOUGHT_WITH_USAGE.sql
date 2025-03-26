{{ 
  config(
   materialized='incremental',
   incremental_strategy='merge', 
   table_format='iceberg', 
   unique_key=['CUST_OPER_CD', 'CUST_CTRY_CD', 'MONTHSTART', 'PRODUCT', 'ACCESS_NUMBER'] ,
   post_hook=" {{ drop_tables_by_prefix('q2_')}}"
) 
}}

SELECT * from {{ref('Q2_CCOM_NBRS_BOUGHT_WITH_USAGE')}} 
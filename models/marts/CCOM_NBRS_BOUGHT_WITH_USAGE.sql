{{ 
  config(
   materialized='incremental',
   incremental_strategy='append', 
   unique_key=['CUST_OPER_CD', 'PRODUCT', 'ACCESS_NUMBER'],
   post_hook=" {{ drop_tables_by_prefix('q2_')}}"
) 
}}

SELECT * from {{ref('Q2_CCOM_NBRS_BOUGHT_WITH_USAGE')}} AS 
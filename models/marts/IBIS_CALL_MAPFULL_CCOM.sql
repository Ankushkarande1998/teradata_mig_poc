{{ 
  config(
   materialized='incremental',
   incremental_strategy='append', 
   unique_key=['IN_CDR_FILE_ID','IN_CDR_SERIAL_NR','OUT_CDR_FILE_ID', 'OUT_CDR_SERIAL_NR']
) 
}}

select * from {{ref('Q1_IBIS_CALL_MAPFULL_CCOM_RATING_ORIGINAL_PRODUCT')}}
-- In case of only one sql file we need to refer to Q2 table



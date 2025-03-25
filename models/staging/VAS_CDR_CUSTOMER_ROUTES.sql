{{ config(materialized='ephemeral') }}

SELECT * From {{source('DWHIBIS','VAS_CDR_CUSTOMER_ROUTES')}}
{{ config(materialized='ephemeral') }}

SELECT * From {{source('DWHIBIS','IBIS_B_EXCHANGE_RATE_DAY')}}
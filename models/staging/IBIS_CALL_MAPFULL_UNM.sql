{{ config(materialized='ephemeral') }}

SELECT * From {{source('DWHIBIS','IBIS_CALL_MAPFULL_UNM')}}
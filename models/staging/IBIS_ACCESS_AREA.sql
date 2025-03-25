{{ config(materialized='ephemeral') }}

SELECT * From {{source('DWHIBIS','IBIS_ACCESS_AREA')}}
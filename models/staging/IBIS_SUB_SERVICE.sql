{{ config(materialized='ephemeral') }}

SELECT * From {{source('DWHIBIS','IBIS_SUB_SERVICE')}}
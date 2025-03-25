{{ config(materialized='ephemeral') }}

SELECT * From {{source('DWHIBIS','IBIS_B_TIME_DIFF_INTERVAL')}}
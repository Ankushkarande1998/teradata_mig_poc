{{ config(materialized='ephemeral') }}

SELECT * From {{source('DWHIBIS','VAS_EXT_DM_RPT_SRV')}}
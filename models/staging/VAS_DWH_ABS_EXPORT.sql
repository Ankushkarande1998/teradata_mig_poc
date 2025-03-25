{{ config(materialized='ephemeral') }}

SELECT * From {{source('DWHIBIS','VAS_DWH_ABS_EXPORT')}}
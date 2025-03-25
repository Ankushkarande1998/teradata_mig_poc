{{ config(materialized='ephemeral') }}

SELECT * From {{source('DWHIBIS','TRUNKGROUP_TERM')}}
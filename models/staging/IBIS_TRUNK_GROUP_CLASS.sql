{{ config(materialized='ephemeral') }}

SELECT * From {{source('DWHIBIS','IBIS_TRUNK_GROUP_CLASS')}}
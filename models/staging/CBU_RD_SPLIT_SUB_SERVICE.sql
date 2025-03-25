{{ config(materialized='ephemeral') }}

SELECT * From {{source('DWHIBIS','CBU_RD_SPLIT_SUB_SERVICE')}}
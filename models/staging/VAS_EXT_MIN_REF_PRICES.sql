{{ config(materialized='ephemeral') }}

SELECT * From {{source('DWHIBIS','VAS_EXT_MIN_REF_PRICES')}}
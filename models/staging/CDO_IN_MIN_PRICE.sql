{{ config(materialized='ephemeral') }}

SELECT * From {{source('DWHIBIS','CDO_IN_MIN_PRICE')}}
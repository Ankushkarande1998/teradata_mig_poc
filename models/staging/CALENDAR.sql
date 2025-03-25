{{ config(materialized='ephemeral') }}

SELECT * From {{source('DWHODS','CALENDAR')}}
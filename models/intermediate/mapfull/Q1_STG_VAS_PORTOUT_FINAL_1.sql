
{{ config(
    materialized='table',
    pre_hook="DROP TABLE IF EXISTS {{ this }}"
    ) 
}}
SELECT -1 AS PK_ID,
        ACCESS_NUMBER,
        LINK_ID,
        ORIGIN_COUNTRY,
        PORT_OUT,
        START_DATE,
        END_DATE
FROM {{ref('Q1_STG_VAS_PORTOUT_OVERLAP_MERGED')}}

union

SELECT  PK_ID,
        ACCESS_NUMBER,
        -1 as LINK_ID,
        ORIGIN_COUNTRY,
        PORT_OUT,
        START_DATE,
        END_DATE
FROM {{ref('Q1_STG_VAS_PORTOUT_FINAL')}}


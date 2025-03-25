--file 14
{{ config(
    materialized='table',
    pre_hook="DROP TABLE IF EXISTS {{ this }}"
    ) 
}}

SELECT  -1 AS PK_ID,
        R1.ACCESS_NUMBER,
        R1.LINK_ID,
        R1.ORIGIN_COUNTRY,
        R1.PORT_IN,
        R1.START_DATE,
        COALESCE(R1.END_DATE, TO_DATE('3000-12-31')) AS END_DATE
FROM {{ref('VAS_EXT_DM_RPT')}} R1
WHERE R1.NUMBER_STATUS IN ('PROVISIONED', 'PRE_PROVISIONED')

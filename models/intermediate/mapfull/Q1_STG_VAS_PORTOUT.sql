--file 18
{{ config(
    materialized='table',
    pre_hook="DROP TABLE IF EXISTS {{ this }}"
    ) 
}}

-- I suggest that we create a separate table with the following data: (to be checked in the future, if needed)
-- CALL_YEAR_MOTH + ACCESS_NUMBER + PORT_OUT_FLAG (Meaning => this number has been ported_out during this month)
-- (to be checked in the future, if needed) otherwise the QLik Developper needs to do a ad-hoc query directly 
-- ino the VAS source Table
SELECT  -1 AS PK_ID,
        R1.ACCESS_NUMBER,
        R1.LINK_ID,
        R1.ORIGIN_COUNTRY,
        R1.PORT_OUT,
        R1.START_DATE,
        COALESCE(R1.END_DATE, TO_DATE('3000-12-31')) AS END_DATE
FROM   {{ref('VAS_EXT_DM_RPT')}} R1
WHERE  PORT_OUT = 1;
-- File: 5

{{ config(
    materialized='table',
    pre_hook="DROP TABLE IF EXISTS {{ this }}"
    ) 
}}


-- In case of double mappings, put the duplicates in a separate table
-- IN CDR
WITH duplicates_in AS (
    SELECT * 
    FROM {{ ref('Q2_IBIS_CALL_MAPFULL_CCOM_RATING') }}
    WHERE (IN_CDR_FILE_ID, IN_CDR_SERIAL_NR, COALESCE(IN_NUMBER_OF_UNITS,0)) IN
     (
    SELECT  IN_CDR_FILE_ID, IN_CDR_SERIAL_NR, COALESCE(IN_NUMBER_OF_UNITS,0)
    FROM {{ ref('Q2_IBIS_CALL_MAPFULL_CCOM_RATING') }}
      WHERE   IN_ACCOUNT_OPER_ID IS NOT NULL 
       AND IN_CDR_FILE_ID <> -1
        --AND FLAG_IND <> 'V'
    GROUP BY 1,2,3 HAVING COUNT(*) > 1
    )
),

-- OUT CDR
duplicates_out AS (
    SELECT *
    FROM {{ ref('Q2_IBIS_CALL_MAPFULL_CCOM_RATING') }}
    WHERE (OUT_CDR_FILE_ID, OUT_CDR_SERIAL_NR, COALESCE(OUT_NUMBER_OF_UNITS,0)) IN
      (
    SELECT  OUT_CDR_FILE_ID, OUT_CDR_SERIAL_NR, COALESCE(OUT_NUMBER_OF_UNITS,0)
    FROM  {{ ref('Q2_IBIS_CALL_MAPFULL_CCOM_RATING') }}
    WHERE OUT_ACCOUNT_OPER_ID IS NOT NULL
       AND OUT_CDR_FILE_ID <> -1
    --   AND FLAG_IND <> 'V'
    GROUP BY 1,2,3 HAVING COUNT(*) > 1
    )
)

SELECT dup_in.* 
FROM duplicates_in dup_in
UNION ALL
SELECT dup_out.*
FROM duplicates_out dup_out

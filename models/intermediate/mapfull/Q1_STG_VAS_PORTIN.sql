--file 14
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key='PK_ID',
    merge_update_columns=['ACCESS_NUMBER', 'LINK_ID', 'ORIGIN_COUNTRY', 'PORT_IN', 'START_DATE', 'END_DATE'],
    pre_hook=["DROP TABLE IF EXISTS {{ this }};",
                "CREATE TABLE IF NOT EXISTS {{this}} (
                    PK_ID BIGINT NOT NULL PRIMARY KEY,
                    ACCESS_NUMBER STRING NOT NULL,
                    LINK_ID INT,
                    ORIGIN_COUNTRY STRING,
                    PORT_IN INT,
                    START_DATE TIMESTAMP,
                    END_DATE TIMESTAMP
                ) USING DELTA;
        "
    ]
    ) 
}}

SELECT  ROW_NUMBER() OVER (ORDER BY ACCESS_NUMBER) AS PK_ID,
        R1.ACCESS_NUMBER,
        R1.LINK_ID,
        R1.ORIGIN_COUNTRY,
        R1.PORT_IN,
        R1.START_DATE,
        COALESCE(R1.END_DATE, TO_DATE('3000-12-31')) AS END_DATE
FROM {{ref('VAS_EXT_DM_RPT')}} R1
WHERE R1.NUMBER_STATUS IN ('PROVISIONED', 'PRE_PROVISIONED')

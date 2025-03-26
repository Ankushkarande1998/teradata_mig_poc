--file 22
{{ config(
    materialized = 'table',
    pre_hook = "DROP TABLE IF EXISTS {{ this }}"
) }}
SELECT
    wk.CALL_DT,
    wk.A_NR,
    wk.A_NR_To_Join,
    wk.CTRY_To_Join,
    wk.IN_CDR_FILE_ID,
    wk.IN_CDR_SERIAL_NR,
    wk.OUT_CDR_FILE_ID,
    wk.OUT_CDR_SERIAL_NR,
    wk.LINK_ID,
    wk.VAS_NAT_OUT_CALL_SRV as VAS_NAT_OUT_CALL_SRV_FLAG,
    --column in sql file
    wk.VAS_PORT_IN,
    wk.VAS_PORT_OUT,
    wk.DIRECTION,
    wk.IN_VAS_ACCESS_NUMBER,
    wk.OUT_VAS_ACCESS_NUMBER
FROM
    (
        SELECT
            A.CALL_DT,
            A.A_NR,
            A.A_NR_To_Join,
            A.CTRY_To_Join,
            A.IN_CDR_FILE_ID,
            A.IN_CDR_SERIAL_NR,
            A.OUT_CDR_FILE_ID,
            A.OUT_CDR_SERIAL_NR,
            RPT_PORTIN.LINK_ID,
            MAX(
                CASE
                    WHEN SRV.ACCESS_NUMBER IS NOT NULL THEN CASE
                        WHEN SRV.SERVICE_DESCRIPTION = 'National Outbound Call' THEN SRV.IS_ACTIVE
                        ELSE NULL
                    END
                    ELSE NULL
                END
            ) AS VAS_NAT_OUT_CALL_SRV,
            RPT_PORTIN.PORT_IN AS VAS_PORT_IN,
            CASE
                WHEN RPT_PORTOUT.PORT_OUT IS NULL THEN 0
                ELSE RPT_PORTOUT.PORT_OUT
            END AS VAS_PORT_OUT,
            A.DIRECTION,
            A.IN_VAS_ACCESS_NUMBER,
            A.OUT_VAS_ACCESS_NUMBER
        FROM
            (
                select
                    *,
                    CASE
                        WHEN DIRECTION = 'OUTBOUND' THEN A_NR
                        ELSE COALESCE(IN_VAS_ACCESS_NUMBER, OUT_VAS_ACCESS_NUMBER)
                    END AS A_NR_To_Join,
                    CASE
                        WHEN DIRECTION = 'OUTBOUND' THEN A_NR_CTRY_CD
                        ELSE B_NR_CTRY_CD
                    END AS CTRY_To_Join
                from
                    {{ ref(
                        'Q1_IBIS_CALL_MAPFULL_CCOM_RATING_updated_fields'
                    ) }}
            ) A
            LEFT OUTER JOIN (
                SELECT
                    R2.ACCESS_NUMBER,
                    R2.LINK_ID,
                    R2.ORIGIN_COUNTRY,
                    R2.PORT_IN,
                    R2.START_DATE,
                    R2.END_DATE
                FROM
                    {{ ref('Q1_STG_VAS_PORTIN_FINAL_1') }} R2
            ) RPT_PORTIN ON 1 = 1
            AND A.CALL_DT BETWEEN RPT_PORTIN.START_DATE
            AND RPT_PORTIN.END_DATE
            AND A_NR_To_Join = RPT_PORTIN.ACCESS_NUMBER
            AND CTRY_To_Join = RPT_PORTIN.ORIGIN_COUNTRY
            LEFT OUTER JOIN {{ ref('VAS_EXT_DM_RPT_SRV') }} SRV ON SRV.ACCESS_NUMBER = RPT_PORTIN.ACCESS_NUMBER
            AND RPT_PORTIN.LINK_ID IS NOT NULL
            AND SRV.LINK_ID = RPT_PORTIN.LINK_ID
            AND SRV.SERVICE_DESCRIPTION = 'National Outbound Call'
            AND A.CALL_DT BETWEEN COALESCE(CAST(SRV.VALID_FROM AS DATE), DATE('1900-01-01'))
            AND COALESCE(CAST(SRV.VALID_TO AS DATE), DATE('1900-01-01'))
            LEFT OUTER JOIN (
                SELECT
                    R3.ACCESS_NUMBER,
                    R3.LINK_ID,
                    R3.ORIGIN_COUNTRY,
                    R3.PORT_OUT,
                    R3.START_DATE,
                    R3.END_DATE
                FROM
                    {{ ref('Q1_STG_VAS_PORTOUT_FINAL_1') }} R3
            ) RPT_PORTOUT ON 1 = 1
            AND A.CALL_DT BETWEEN RPT_PORTOUT.START_DATE
            AND RPT_PORTOUT.END_DATE --AND A.CALL_DT >= RPT_PORTOUT.START_DATE
            AND A_NR_To_Join = RPT_PORTOUT.ACCESS_NUMBER
            AND CTRY_To_Join = RPT_PORTOUT.ORIGIN_COUNTRY
        WHERE
            (
                A.IN_VAS_ACCESS_NUMBER IS NOT NULL
                OR A.OUT_VAS_ACCESS_NUMBER IS NOT NULL
            )
        GROUP BY
            1,2,3,4,5,6,7,8,9,11,12,13,14,15
    ) wk QUALIFY ROW_NUMBER () OVER (
        PARTITION BY wk.CALL_DT,
        wk.A_NR,
        wk.A_NR_To_Join,
        wk.CTRY_To_Join,
        wk.IN_CDR_FILE_ID,
        wk.IN_CDR_SERIAL_NR,
        wk.OUT_CDR_FILE_ID,
        wk.OUT_CDR_SERIAL_NR
        ORDER BY
            wk.LINK_ID DESC
    ) = 1
{{ set_model_config() }}

WITH source_calls AS (
    SELECT
        *,
        CASE
            WHEN DIRECTION = 'OUTBOUND' THEN A_NR
            ELSE COALESCE(IN_VAS_ACCESS_NUMBER, OUT_VAS_ACCESS_NUMBER)
        END AS A_NR_To_Join,
        CASE
            WHEN DIRECTION = 'OUTBOUND' THEN A_NR_CTRY_CD
            ELSE B_NR_CTRY_CD
        END AS CTRY_To_Join
    FROM {{ ref('Q1_IBIS_CALL_MAPFULL_CCOM_RATING_updated_fields') }}
),

portin_data AS (
    SELECT
        ACCESS_NUMBER,
        LINK_ID,
        ORIGIN_COUNTRY,
        PORT_IN,
        START_DATE,
        END_DATE
    FROM {{ ref('Q1_STG_VAS_PORTIN_FINAL_1') }}
),

portout_data AS (
    SELECT
        ACCESS_NUMBER,
        LINK_ID,
        ORIGIN_COUNTRY,
        PORT_OUT,
        START_DATE,
        END_DATE
    FROM {{ ref('Q1_STG_VAS_PORTOUT_FINAL_1') }}
),

srv_data AS (
    SELECT
        ACCESS_NUMBER,
        LINK_ID,
        SERVICE_DESCRIPTION,
        IS_ACTIVE,
        VALID_FROM,
        VALID_TO
    FROM {{ ref('VAS_EXT_DM_RPT_SRV') }}
    WHERE SERVICE_DESCRIPTION = 'National Outbound Call'
),

joined_data_with_rn AS (
    SELECT
        A.CALL_DT,
        A.A_NR,
        A.A_NR_To_Join,
        A.CTRY_To_Join,
        A.IN_CDR_FILE_ID,
        A.IN_CDR_SERIAL_NR,
        A.OUT_CDR_FILE_ID,
        A.OUT_CDR_SERIAL_NR,
        PI.LINK_ID,
        MAX(
            CASE
                WHEN SRV.ACCESS_NUMBER IS NOT NULL THEN
                    CASE
                        WHEN SRV.SERVICE_DESCRIPTION = 'National Outbound Call' THEN SRV.IS_ACTIVE
                        ELSE NULL
                    END
                ELSE NULL
            END
        ) AS VAS_NAT_OUT_CALL_SRV,
        PI.PORT_IN AS VAS_PORT_IN,
        COALESCE(PO.PORT_OUT, 0) AS VAS_PORT_OUT,
        A.DIRECTION,
        A.IN_VAS_ACCESS_NUMBER,
        A.OUT_VAS_ACCESS_NUMBER,
        ROW_NUMBER() OVER (
            PARTITION BY
                A.CALL_DT,
                A.A_NR,
                A.A_NR_To_Join,
                A.CTRY_To_Join,
                A.IN_CDR_FILE_ID,
                A.IN_CDR_SERIAL_NR,
                A.OUT_CDR_FILE_ID,
                A.OUT_CDR_SERIAL_NR
            ORDER BY PI.LINK_ID DESC
        ) AS rn
    FROM source_calls A
    LEFT JOIN portin_data PI
        ON A.CALL_DT BETWEEN PI.START_DATE AND PI.END_DATE
        AND A.A_NR_To_Join = PI.ACCESS_NUMBER
        AND A.CTRY_To_Join = PI.ORIGIN_COUNTRY
    LEFT JOIN srv_data SRV
        ON SRV.ACCESS_NUMBER = PI.ACCESS_NUMBER
        AND SRV.LINK_ID = PI.LINK_ID
        AND A.CALL_DT BETWEEN COALESCE(CAST(SRV.VALID_FROM AS DATE), DATE('1900-01-01'))
                         AND COALESCE(CAST(SRV.VALID_TO AS DATE), DATE('1900-01-01'))
    LEFT JOIN portout_data PO
        ON A.CALL_DT BETWEEN PO.START_DATE AND PO.END_DATE
        AND A.A_NR_To_Join = PO.ACCESS_NUMBER
        AND A.CTRY_To_Join = PO.ORIGIN_COUNTRY
    WHERE A.IN_VAS_ACCESS_NUMBER IS NOT NULL OR A.OUT_VAS_ACCESS_NUMBER IS NOT NULL
    GROUP BY
        A.CALL_DT, A.A_NR, A.A_NR_To_Join, A.CTRY_To_Join,
        A.IN_CDR_FILE_ID, A.IN_CDR_SERIAL_NR,
        A.OUT_CDR_FILE_ID, A.OUT_CDR_SERIAL_NR,
        PI.LINK_ID, PI.PORT_IN, PO.PORT_OUT,
        A.DIRECTION, A.IN_VAS_ACCESS_NUMBER, A.OUT_VAS_ACCESS_NUMBER
)

SELECT
    CALL_DT,
    A_NR,
    A_NR_To_Join,
    CTRY_To_Join,
    IN_CDR_FILE_ID,
    IN_CDR_SERIAL_NR,
    OUT_CDR_FILE_ID,
    OUT_CDR_SERIAL_NR,
    LINK_ID,
    VAS_NAT_OUT_CALL_SRV AS VAS_NAT_OUT_CALL_SRV_FLAG,
    VAS_PORT_IN,
    VAS_PORT_OUT,
    DIRECTION,
    IN_VAS_ACCESS_NUMBER,
    OUT_VAS_ACCESS_NUMBER
FROM joined_data_with_rn
WHERE rn = 1
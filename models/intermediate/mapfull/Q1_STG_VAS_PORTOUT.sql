{{set_starburst_portout_config()}}

-- Optional note: If this table is useful in the future, consider persisting CALL_YEAR_MONTH + ACCESS_NUMBER + PORT_OUT_FLAG
-- Otherwise, Qlik developers may query the source VAS_EXT_DM_RPT table directly.

SELECT  
    ROW_NUMBER() OVER (ORDER BY ACCESS_NUMBER) AS PK_ID,
    R1.ACCESS_NUMBER,
    R1.LINK_ID,
    R1.ORIGIN_COUNTRY,
    R1.PORT_OUT,
    R1.START_DATE,
    COALESCE(R1.END_DATE, DATE '3000-12-31') AS END_DATE
FROM {{ ref('VAS_EXT_DM_RPT') }} R1
WHERE R1.PORT_OUT = 1

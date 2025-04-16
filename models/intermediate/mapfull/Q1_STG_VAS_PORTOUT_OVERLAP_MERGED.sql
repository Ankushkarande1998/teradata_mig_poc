{{ set_model_config() }}

-- Now, for the remaing records that are overlapped (if any), we need to merge them
-- As we will have Data Quality issues, we will do a MAX on LINK_ID and PORT_IN

SELECT  wk.ACCESS_NUMBER, 
        MAX(COALESCE(TRY_CAST(wk.LINK_ID as BIGINT), -1)) AS LINK_ID,
        wk.ORIGIN_COUNTRY,
        MAX(wk.PORT_OUT) AS PORT_OUT,
        wk.DATESTART as START_DATE,
        wk.DATEEND as END_DATE
FROM 
    (
    SELECT  wk1.ACCESS_NUMBER,
            wk1.ORIGIN_COUNTRY,
            COALESCE(TRY_CAST(wk1.LINK_ID as BIGINT), -1) AS LINK_ID,
            MIN(MINDATESTART) OVER(PARTITION BY wk1.ACCESS_NUMBER, wk1.ORIGIN_COUNTRY, 
                 wk1.MAXDATEEND) AS DATESTART,
            MAX(MAXDATEEND) OVER(PARTITION BY wk1.ACCESS_NUMBER, wk1.ORIGIN_COUNTRY, 
                 wk1.MINDATESTART) AS DATEEND,
            wk1.PORT_OUT
    FROM 
        (
        SELECT  L.ACCESS_NUMBER,
                L.ORIGIN_COUNTRY,
                COALESCE(TRY_CAST(L.LINK_ID as BIGINT), -1) AS LINK_ID,
                MIN(R.START_DATE) OVER(PARTITION BY L.ACCESS_NUMBER, L.ORIGIN_COUNTRY, 
                    L.END_DATE) MINDATESTART,
                MAX(R.END_DATE) OVER(PARTITION BY L.ACCESS_NUMBER, L.ORIGIN_COUNTRY, 
                    L.END_DATE) MAXDATEEND,
                L.PORT_OUT
        FROM {{ref('Q1_STG_VAS_PORTOUT')}} L 
        LEFT JOIN {{ref('Q1_STG_VAS_PORTOUT')}} R
            ON L.ACCESS_NUMBER = R.ACCESS_NUMBER
           AND L.ORIGIN_COUNTRY = R.ORIGIN_COUNTRY
           AND L.START_DATE <= R.END_DATE
           AND L.END_DATE >= R.START_DATE
        ) wk1
    ) wk
GROUP BY  wk.ACCESS_NUMBER,
          wk.ORIGIN_COUNTRY,
          wk.DATESTART,
          wk.DATEEND




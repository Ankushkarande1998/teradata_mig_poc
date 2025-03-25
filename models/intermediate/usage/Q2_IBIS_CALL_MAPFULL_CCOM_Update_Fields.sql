--file 13
{{ config(
    materialized='table',
    pre_hook="DROP TABLE IF EXISTS {{ this }}"

    ) 
}}

--   In The Cloud COMM DM Report they want to see FCM in the Product instead of 'unknown'
--   for the time being is NULL. so we need to hardcoded to FCM if i_tgc_id = 13 (M) and
--   TRANSM_OPER_ID = 2195494 (Only for USA/AMAZCO)
-- See Below in the CASE Statment where we build the VAS_PRODUCT
SELECT  CCOM.CALL_DT,
        CCOM.IN_CDR_FILE_ID, CCOM.IN_CDR_SERIAL_NR,
        CCOM.OUT_CDR_FILE_ID, CCOM.OUT_CDR_SERIAL_NR,
        CCOM.IN_GLOBAL_CALL_ID,
        CCOM.DIRECTION_tmp AS DIRECTION,
        CCOM.MAP_STATUS,
        CCOM.VAS_PRODUCT AS PRODUCT,
        CCOM.A_NR_CTRY_CD_tmp AS A_NR_CTRY_CD,
        CCOM.B_NR_CTRY_CD_tmp AS B_NR_CTRY_CD,
        CCOM.TRANSM_OPER_ID,
        TRANSM_OPER.CTRY_CD || '/' || TRANSM_OPER.OPER_CD AS TRANSM_OPER,
        CCOM.RECV_OPER_ID,
        CCOM.RECV_OPER,
        CCOM.DEST_OPER_ID_Swap AS DEST_OPER_ID,
        CCOM.DEST_OPER_Swap AS DEST_OPER,
        CCOM.FF_FLAG, -- NOT FF800 / FF/999 / FF/998 Flags = 1
    
        -- Defining the Customer
        CCOM.CUSTOMER_OPER_ID as CUST_OPER_ID_Final,
        CUST_OPER.CTRY_CD || '/' || CUST_OPER.OPER_CD AS CUST_OPER_Final,
    
        -- Defining the Supplier
        CCOM.SUPPL_OPER_ID as SUPPL_OPER_ID_Final,
        SUPPL_OPER.CTRY_CD || '/' ||SUPPL_OPER.OPER_CD AS SUPPL_OPER_Final,
 
        -- Defining Customer & Supplier TGC_ID
        CCOM.I_TGC_ID,
        IN_TGC.TGC_CD  AS IN_TGC_CD,
        CCOM.O_TGC_ID,
        OUT_TGC.TGC_CD  AS OUT_TGC_CD,
 
        -- New Customer TGC
        CCOM.CUST_TGC_ID_Final,
        CUST_TGC.TGC_CD AS CUST_TGC_CD_Final,
 
        -- New Supplier TGC
        CCOM.SUPPL_TGC_ID_Final,
        SUPPL_TGC.TGC_CD AS SUPPL_TGC_CD_Final,
 
        -- Defining Customer & Supplier Currency_ID
        CCOM.IN_CURRENCY_ID,
        IN_IC.CURRENCY_CD AS IN_CURRENCY_CD,
        CCOM.OUT_CURRENCY_ID,
        OUT_IC.CURRENCY_CD AS OUT_CURRENCY_CD,
 
        -- Customer Currency
        CCOM. CUST_CURRENCY_ID_Final,
        IC_CUST.CURRENCY_CD AS CUST_CURRENCY_CD_Final,
 
        -- Supplier Currency
        CCOM.SUPPL_CURRENCY_ID_Final,
        IC_SUPPL.CURRENCY_CD AS SUPPL_CURRENCY_CD_Final,
 
        -- Defining the Customer & Supplier DEST_SUB_SVC_ID
        CCOM.IN_DEST_SUB_SVC_ID,
        IN_DEST_SUBS.SUB_SVC_CD AS IN_DEST_SUBS_SVC_CD,
        CCOM.OUT_DEST_SUB_SVC_ID,
        OUT_DEST_SUBS.SUB_SVC_CD AS OUT_DEST_SUBS_SVC_CD,
 
        -- Customer DEST_SUB_SVC_ID
        CCOM.CUST_DEST_SUB_SVC_ID_Final,
        ISS_CUST.SUB_SVC_CD AS CUST_DEST_SUB_SVC_CD_Final,
 
        -- Supplier DEST_SUB_SVC_ID
        CCOM.SUPPL_DEST_SUB_SVC_ID_Final,
        ISS_SUPPL.SUB_SVC_CD AS SUPPL_DEST_SUB_SVC_CD_Final,
 
        -- Define ORIG_OPER
        CCOM.ADD_ORIG_OPER_ID AS ORIG_OPER_ID_Final,
        CCOM.ADD_ORIG_OPER_CTRY_CD || '/' || CCOM.ADD_ORIG_OPER_CD AS ORIG_OPER_Final
 
FROM
    (
    SELECT  CC.CALL_DT,
            CC.IN_GLOBAL_CALL_ID,
            CC.IN_CDR_FILE_ID, CC.IN_CDR_SERIAL_NR,
            CC.OUT_CDR_FILE_ID, CC.OUT_CDR_SERIAL_NR,
            COALESCE(COALESCE(CC.IN_VAS_DIRECTION,CC.OUT_VAS_DIRECTION),'OUTBOUND') AS DIRECTION_tmp,
            CC.MAP_STATUS,
            CASE
                WHEN CC.IN_VAS_DIRECTION IS NULL AND CC.OUT_VAS_DIRECTION IS NULL AND (CC.O_TGC_ID=724 OR CC.I_TGC_ID=724) THEN 'SIPT'
                WHEN CC.IN_VAS_DIRECTION IS NULL AND CC.OUT_VAS_DIRECTION IS NULL AND (CC.O_TGC_ID=619 OR CC.I_TGC_ID=619) THEN 'GMN'
                WHEN CC.IN_VAS_PRODUCT IS NULL AND CC.OUT_VAS_PRODUCT IS NOT NULL
                        THEN CC.OUT_VAS_PRODUCT
                        ELSE CC.IN_VAS_PRODUCT
            END AS VAS_PRODUCT,
 
            -- For OUTBOUND --> CTRY_CD from ADD_ORIG_OPER_ID
            -- For INBOUND + FF800 (IN) OR FF800(OUT) --> RECV_CTRY_CD or DEST_CTRY_CD
            --     OTHERWISE --> ABS_EXP.ORIGIN_CTRY_CD
 
            CASE  WHEN DIRECTION_tmp = 'OUTBOUND'
                     THEN  ADD_ORIG_OPER.CTRY_CD
                  WHEN DIRECTION_tmp = 'INBOUND' AND CC.DEST_SUB_SVC_CD = 'FF800'
                     --AND COALESCE(IN_DEST_SUB_SVC_ID,0) = 2107838 -- FF800
                     --AND COALESCE(OUT_DEST_SUB_SVC_ID,0) = 2107838 -- FF800
                        -- If Dummy Operator (Ex: USA/USA or If OPER_ID = 0 (NO/TKG) then RECV else DEST
                        THEN CASE WHEN (DEST_OPER.CTRY_CD = DEST_OPER.OPER_CD) OR DEST_OPER.OPER_ID = 0
                                    THEN RECV_OPER.CTRY_CD
                                    ELSE DEST_OPER.CTRY_CD
                                 END
                         ELSE ABS_EXP.ORIGIN_CTRY_CD
            END AS A_NR_CTRY_CD_tmp,
 
            CASE WHEN IN_VAS_DIRECTION IS NULL AND OUT_VAS_DIRECTION IS NULL
                    -- If Dummy Operator (Ex: USA/USA or If OPÃ‹R_ID = 0 (NO/TKG) then RECV else DEST
                    THEN CASE WHEN (DEST_OPER.CTRY_CD = DEST_OPER.OPER_CD) OR DEST_OPER.OPER_ID = 0
                                    THEN RECV_OPER.CTRY_CD
                                    ELSE DEST_OPER.CTRY_CD
                         END
                    ELSE COALESCE(CC.IN_VAS_B_NUMBER_CTRY_CD, CC.OUT_VAS_B_NUMBER_CTRY_CD)
            END AS B_NR_CTRY_CD_tmp,
 
            CC.TRANSM_OPER_ID,
            CC.RECV_OPER_ID,
            RECV_OPER.CTRY_CD || '/' || RECV_OPER.OPER_CD AS RECV_OPER,
            -- If Dummy Operator (Ex: USA/USA or If OPER_ID = 0 (NO/TKG) then RECV else DEST
            CASE WHEN (DEST_OPER.CTRY_CD = DEST_OPER.OPER_CD) OR DEST_OPER.OPER_ID = 0
                    THEN RECV_OPER_ID
                    ELSE DEST_OPER.OPER_ID
            END AS DEST_OPER_ID_Swap,
 
            -- If Dummy Operator (Ex: USA/USA or If OPER_ID = 0 (NO/TKG) then RECV else DEST
            CASE WHEN (DEST_OPER.CTRY_CD = DEST_OPER.OPER_CD) OR DEST_OPER.OPER_ID = 0
                    THEN RECV_OPER
                    ELSE DEST_OPER.CTRY_CD || '/' || DEST_OPER.OPER_CD
            END AS DEST_OPER_Swap,
 
            CASE WHEN COALESCE(IN_DEST_SUB_SVC_ID, 0) NOT IN (2107838,2113163,2113162)
                  AND COALESCE(OUT_DEST_SUB_SVC_ID, 0) NOT IN (2107838,2113163,2113162)
                      THEN 1
                    ELSE 0
            END AS FF_FLAG, -- FF800 / FF/999 / FF/998
 
            CC.I_TGC_ID,
            CC.O_TGC_ID,
            IN_CURRENCY_ID,
            OUT_CURRENCY_ID,
            CC.IN_DEST_SUB_SVC_ID,
            CC.OUT_DEST_SUB_SVC_ID,
            CC.ADD_ORIG_OPER_ID,
            --CC.ORIG_OPER_ID,
            ADD_ORIG_OPER.CTRY_CD AS ADD_ORIG_OPER_CTRY_CD,
            ADD_ORIG_OPER.OPER_CD AS ADD_ORIG_OPER_CD,
 
             -- Defining the Customer
            CASE WHEN DIRECTION = 'INBOUND' AND FF_FLAG = 1
                  THEN CASE WHEN (DEST_OPER.CTRY_CD = DEST_OPER.OPER_CD) OR DEST_OPER.OPER_ID = 0
                    THEN RECV_OPER_ID
                    ELSE DEST_OPER.OPER_ID
                    END
                  ELSE CC.TRANSM_OPER_ID
            END AS CUSTOMER_OPER_ID,
        
            -- Defining the Supplier
            CASE WHEN DIRECTION = 'INBOUND' AND FF_FLAG = 1
                    THEN CC.TRANSM_OPER_ID
                    ELSE CC.RECV_OPER_ID
            END AS SUPPL_OPER_ID,
 
            -- New Customer TGC
            CASE WHEN DIRECTION = 'INBOUND' AND FF_FLAG = 0 --(FLAG = 0 means FF800 / FF/999 / FF/998)
                    THEN CC.O_TGC_ID
                    ELSE CC.I_TGC_ID -- OUTBOUND or ('INBOUND' AND FF_FLAG = 1)
            END AS CUST_TGC_ID_Final,
 
            -- New Supplier TGC
            CASE WHEN DIRECTION = 'INBOUND' AND FF_FLAG = 0 --(FLAG = 0 means FF800 / FF/999 / FF/998)
                    THEN CC.I_TGC_ID
                    ELSE CC.O_TGC_ID -- OUTBOUND or ('INBOUND' AND FF_FLAG = 1)
            END AS SUPPL_TGC_ID_Final,
 
            -- Customer Currency
            CASE WHEN DIRECTION = 'INBOUND' AND FF_FLAG = 1
                    THEN CC.OUT_CURRENCY_ID
                    ELSE CC.IN_CURRENCY_ID
            END AS CUST_CURRENCY_ID_Final,
 
            -- Supplier Currency
            CASE WHEN DIRECTION = 'INBOUND' AND FF_FLAG = 1
                    THEN CC.IN_CURRENCY_ID
                    ELSE CC.OUT_CURRENCY_ID
            END AS SUPPL_CURRENCY_ID_Final,
 
            -- Customer DEST_SUB_SVC_ID
            CASE WHEN DIRECTION = 'INBOUND' AND FF_FLAG = 1
                    THEN CC.IN_DEST_SUB_SVC_ID
                    ELSE CC.OUT_DEST_SUB_SVC_ID
            END AS CUST_DEST_SUB_SVC_ID_Final,
 
            -- Supplier DEST_SUB_SVC_ID
            CASE WHEN DIRECTION = 'INBOUND' AND FF_FLAG = 1
                    THEN CC.OUT_DEST_SUB_SVC_ID
                    ELSE CC.IN_DEST_SUB_SVC_ID
            END AS SUPPL_DEST_SUB_SVC_ID_Final

    --FROM DWHIBIS.IBIS_CALL_MAPFULL_CCOM CC
    FROM  {{ref('Q1_IBIS_CALL_MAPFULL_CCOM_RATING_DEDUP_POP')}} CC
    LEFT OUTER JOIN {{ref('IBIS_B_OPERATOR')}} DEST_OPER
        ON COALESCE(CC.IN_DEST_OPER_ID, CC.OUT_DEST_OPER_ID) = DEST_OPER.OPER_ID
    LEFT OUTER JOIN {{ref('IBIS_B_OPERATOR')}} RECV_OPER
        ON CC.RECV_OPER_ID = RECV_OPER.OPER_ID
    LEFT OUTER JOIN {{ref('IBIS_B_OPERATOR')}} ADD_ORIG_OPER
       ON ADD_ORIG_OPER_ID = ADD_ORIG_OPER.OPER_ID
    LEFT OUTER JOIN {{ref('VAS_DWH_ABS_EXPORT')}} ABS_EXP
        ON CC.DEST_SUB_SVC_CD = ABS_EXP.SUB_SERVICE
    ) CCOM
-- TGCs
LEFT JOIN {{ref('IBIS_TRUNK_GROUP_CLASS')}} IN_TGC
    ON CCOM.I_TGC_ID = IN_TGC.TGC_ID
LEFT JOIN {{ref('IBIS_TRUNK_GROUP_CLASS')}} OUT_TGC
    ON CCOM.O_TGC_ID = OUT_TGC.TGC_ID
LEFT JOIN {{ref('IBIS_TRUNK_GROUP_CLASS')}} CUST_TGC
    ON CUST_TGC_ID_Final = CUST_TGC.TGC_ID
LEFT JOIN {{ref('IBIS_TRUNK_GROUP_CLASS')}}  SUPPL_TGC
    ON SUPPL_TGC_ID_Final = SUPPL_TGC.TGC_ID
-- DEST_SUB_SVCs
LEFT JOIN {{ref('IBIS_SUB_SERVICE')}} IN_DEST_SUBS
    ON CCOM.IN_DEST_SUB_SVC_ID = IN_DEST_SUBS.SUB_SVC_ID
LEFT JOIN {{ref('IBIS_SUB_SERVICE')}}  OUT_DEST_SUBS
    ON CCOM.OUT_DEST_SUB_SVC_ID = OUT_DEST_SUBS.SUB_SVC_ID
LEFT JOIN {{ref('IBIS_SUB_SERVICE')}}  ISS_CUST
    ON CUST_DEST_SUB_SVC_ID_Final = ISS_CUST.SUB_SVC_ID
LEFT JOIN {{ref('IBIS_SUB_SERVICE')}}  ISS_SUPPL
    ON SUPPL_DEST_SUB_SVC_ID_Final = ISS_SUPPL.SUB_SVC_ID
-- Operators
LEFT OUTER JOIN {{ref('IBIS_B_OPERATOR')}}  TRANSM_OPER
    ON CCOM.TRANSM_OPER_ID = TRANSM_OPER.OPER_ID
LEFT OUTER JOIN {{ref('IBIS_B_OPERATOR')}}  CUST_OPER
    ON CUSTOMER_OPER_ID = CUST_OPER.OPER_ID
LEFT OUTER JOIN {{ref('IBIS_B_OPERATOR')}}  SUPPL_OPER
    ON SUPPL_OPER_ID = SUPPL_OPER.OPER_ID
-- Currencies
LEFT OUTER JOIN {{ref('IBIS_CURRENCY')}} IN_IC
    ON CCOM.IN_CURRENCY_ID = IN_IC.CURRENCY_ID
LEFT OUTER JOIN {{ref('IBIS_CURRENCY')}} OUT_IC
    ON CCOM.OUT_CURRENCY_ID = OUT_IC.CURRENCY_ID
LEFT OUTER JOIN {{ref('IBIS_CURRENCY')}} IC_CUST
    ON CUST_CURRENCY_ID_Final = IC_CUST.CURRENCY_ID
LEFT OUTER JOIN {{ref('IBIS_CURRENCY')}} IC_SUPPL
    ON SUPPL_CURRENCY_ID_Final = IC_SUPPL.CURRENCY_ID
WHERE 1=1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,
    31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48


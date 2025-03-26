
--file 24
{{ config(
    materialized='table',
    pre_hook="DROP TABLE IF EXISTS {{ this }}"
    ) 
}}


SELECT  A.CALL_DT,                
        A.A_NR,
        A.IN_CDR_FILE_ID,        
        A.IN_CDR_SERIAL_NR,
        A.OUT_CDR_FILE_ID,        
        A.OUT_CDR_SERIAL_NR,
        IN_TGC.TGC_CD  AS IN_TGC_CD,
        OUT_TGC.TGC_CD AS OUT_TGC_CD,
        ISS.SUB_SVC_CD AS IN_DEST_SUB_SVC_CD,
        A.DIRECTION,
        A.PRODUCT,
        A.VAS_NAT_OUT_CALL_SRV_FLAG,
        A.VCCO_OUTBOUND_PRODUCT,
        A.VAS_OUTBOUND_NR_FOUND,        
        CASE WHEN VAS_OUTBOUND_NR_FOUND > 0 THEN 1 ELSE 0 END AS VAS_NR_FOUND,
        CASE WHEN  A.DIRECTION = 'OUTBOUND' AND A.PRODUCT = 'SIPT'
                THEN CASE WHEN VAS_OUTBOUND_NR_FOUND = 1
                        THEN CASE WHEN A.VAS_NAT_OUT_CALL_SRV_FLAG = 1 AND VCCO_OUTBOUND_PRODUCT = 'IBN'
                                      THEN 'SIPT'
                                   WHEN COALESCE(A.VAS_NAT_OUT_CALL_SRV_FLAG, 0) = 0 AND VCCO_OUTBOUND_PRODUCT = 'IBN'
                                      THEN 'IBN'
                                  WHEN VCCO_OUTBOUND_PRODUCT <> 'IBN'
                                      THEN VCCO_OUTBOUND_PRODUCT
                                      ELSE 'Non-BICS DID' -- '11'
                             END
                        WHEN VAS_OUTBOUND_NR_FOUND = 0 AND A.VAS_NAT_OUT_CALL_SRV_FLAG IS NULL
                            THEN 'Non-BICS DID'
                            ELSE 'Non-BICS DID' -- '13'
                     END
            WHEN  A.DIRECTION = 'OUTBOUND' AND A.PRODUCT = 'GMN'
                THEN CASE WHEN VAS_OUTBOUND_NR_FOUND = 1 AND A.VAS_NAT_OUT_CALL_SRV_FLAG = 1
                        AND VCCO_OUTBOUND_PRODUCT = 'IBN'
                                THEN 'SIPT'
                          WHEN VAS_OUTBOUND_NR_FOUND = 1 AND COALESCE(A.VAS_NAT_OUT_CALL_SRV_FLAG, 0) = 0
                        AND VCCO_OUTBOUND_PRODUCT = 'IBN'
                                THEN 'IBN'
                          WHEN VAS_OUTBOUND_NR_FOUND = 1 AND COALESCE(A.VAS_NAT_OUT_CALL_SRV_FLAG, 0) IN (1,0)
                        AND VCCO_OUTBOUND_PRODUCT <> 'IBN'
                                THEN VCCO_OUTBOUND_PRODUCT
                          WHEN COALESCE(VAS_OUTBOUND_NR_FOUND, 0) = 0
                              THEN CASE WHEN A.VAS_NAT_OUT_CALL_SRV_FLAG IS NULL
                                    THEN 'Non-BICS DID'
                                    ELSE 'Non-BICS DID' -- '20'
                                 END
                            ELSE 'Non-BICS DID' -- '21'
                     END
            WHEN  A.DIRECTION = 'OUTBOUND' AND A.PRODUCT IS NULL THEN 'Non-BICS DID'
            WHEN  A.DIRECTION = 'INBOUND'
                THEN A.PRODUCT
        END AS ORIGINAL_PRODUCT
FROM
(
select
      B.* ,
      CASE WHEN VCCO_OUTBOUND.ACCESS_NUMBER IS NOT NULL THEN 1 ELSE 0
        END AS VAS_OUTBOUND_NR_FOUND,
      VCCO_OUTBOUND.PRODUCT AS VCCO_OUTBOUND_PRODUCT
from
 {{ref('Q2_IBIS_CALL_MAPFULL_CCOM_RATING_SERVICES_FLAG')}} B
LEFT OUTER JOIN  {{ref('Q2_VAS_CDR_CUST_ROUTES')}} VCCO_OUTBOUND
--    ON VCCO_OUTBOUND.ACCESS_NUMBER = A.A_NR
    ON CASE WHEN DIRECTION = 'OUTBOUND'
                THEN B.A_NR
                ELSE COALESCE(B.IN_VAS_ACCESS_NUMBER, B.OUT_VAS_ACCESS_NUMBER)
       END = VCCO_OUTBOUND.ACCESS_NUMBER
   AND VCCO_OUTBOUND.CALL_DATE = B.CALL_DT
) A
LEFT JOIN {{ref('IBIS_TRUNK_GROUP_CLASS')}} IN_TGC
    ON A.I_TGC_ID = IN_TGC.TGC_ID
LEFT JOIN {{ref('IBIS_TRUNK_GROUP_CLASS')}} OUT_TGC
    ON A.O_TGC_ID = OUT_TGC.TGC_ID
LEFT OUTER JOIN {{ref('IBIS_SUB_SERVICE')}} ISS
    ON A.IN_DEST_SUB_SVC_ID = ISS.SUB_SVC_ID
 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16;
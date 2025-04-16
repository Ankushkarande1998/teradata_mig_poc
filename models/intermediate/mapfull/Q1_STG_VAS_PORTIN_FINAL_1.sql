{{ set_model_config() }}


( SELECT -1 AS PK_ID,
        ACCESS_NUMBER,
        LINK_ID,
        ORIGIN_COUNTRY,
        PORT_IN,
        START_DATE,
        END_DATE
FROM {{ref('Q1_STG_VAS_PORTIN_OVERLAP_MERGED')}} )
union 
(select PK_ID,
        ACCESS_NUMBER,
       -1 as LINK_ID,
        ORIGIN_COUNTRY,
        PORT_IN,
        START_DATE,
        END_DATE from 
{{ref('Q1_STG_VAS_PORTIN_FINAL')}}
)

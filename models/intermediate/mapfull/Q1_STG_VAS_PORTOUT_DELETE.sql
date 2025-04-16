{{ set_model_config() }}

-- Delete the not Overlapped Records from the first table
-- We will leave only the overlapped ones
SELECT * FROM {{ref('Q1_STG_VAS_PORTOUT')}}
WHERE PK_ID NOT IN (SELECT PK_ID FROM {{ref('Q1_STG_VAS_PORTOUT_FINAL')}})
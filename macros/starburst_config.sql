{% macro set_starburst_config() %}
  {% if target.type == 'trino' %}
    {{ config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key='PK_ID',
    on_table_exists='replace',
    catalog='iceberg' , 
    pre_hook=[
        "DROP TABLE IF EXISTS {{ this.database }}.{{ this.schema }}.{{ this.identifier }}",
        "CREATE TABLE IF NOT EXISTS {{ this }} ( \
            PK_ID BIGINT NOT NULL, \
            ACCESS_NUMBER VARCHAR NOT NULL, \
            LINK_ID INT, \
            ORIGIN_COUNTRY VARCHAR, \
            PORT_IN INT, \
            START_DATE TIMESTAMP, \
            END_DATE TIMESTAMP \
        ) \
        WITH (format = 'PARQUET')"
    ]
) }}
 {% else %}
  {{ config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key='PK_ID',
   
   pre_hook=[
    "DROP TABLE IF EXISTS {{ this.database }}.{{ this.schema }}.{{ this.identifier }}",
    "CREATE TABLE IF NOT EXISTS {{ this }} ( \
        PK_ID BIGINT NOT NULL, \
        ACCESS_NUMBER STRING NOT NULL, \
        LINK_ID INT, \
        ORIGIN_COUNTRY STRING, \
        PORT_IN INT, \
        START_DATE TIMESTAMP, \
        END_DATE TIMESTAMP \
    ) USING DELTA"
]

) }}      
{% endif %}
{% endmacro %}
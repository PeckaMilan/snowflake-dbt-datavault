{{
    config(
        materialized='dynamic_table',
        snowflake_warehouse='COMPUTE_WH',
        target_lag='1 day',
        on_configuration_change='apply'
    )
}}

{#
    SATELLITE: SAT_CUSTOMERS_DETAILS
    Parent Hub: HUB_CUSTOMERS
    Payload: FIRST_NAME, LAST_NAME, EMAIL, CITY, COUNTRY, CREATED_DATE
#}

{%- set payload_columns = [
    'FIRST_NAME',
    'LAST_NAME',
    'CUSTOMER_EMAIL',
    'CITY',
    'COUNTRY',
    'CREATED_DATE'
] -%}

SELECT
    HK_H_CUSTOMERS,
    HASHDIFF,
    {% for col in payload_columns %}
    {{ col }},
    {% endfor %}
    EFFECTIVE_TS,
    DWH_VALID_TS,
    DSS_RECORD_SOURCE
FROM (
    SELECT
        {{ dv_hash(['CUSTOMER_ID'], 'HK_H_CUSTOMERS') }},
        {{ dv_hashdiff(payload_columns, 'HASHDIFF') }},
        {% for col in payload_columns %}
        {{ col }},
        {% endfor %}
        CURRENT_TIMESTAMP() AS EFFECTIVE_TS,
        CURRENT_TIMESTAMP() AS DWH_VALID_TS,
        'DEMO_DB.DBT_DEV.RAW_CUSTOMERS' AS DSS_RECORD_SOURCE
    FROM {{ source('raw_data', 'RAW_CUSTOMERS') }}
    WHERE CUSTOMER_ID IS NOT NULL
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY HK_H_CUSTOMERS, HASHDIFF
    ORDER BY DWH_VALID_TS
) = 1

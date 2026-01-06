{{
    config(
        materialized='dynamic_table',
        snowflake_warehouse='COMPUTE_WH',
        target_lag='1 day',
        on_configuration_change='apply'
    )
}}

{# SATELLITE: SAT_CUSTOMER_DETAILS - Parent Hub: HUB_CUSTOMER #}

SELECT
    HK_H_HUB_CUSTOMER,
    HASHDIFF,
    FIRST_NAME,
        LAST_NAME,
        CUSTOMER_EMAIL,
        CITY,
        COUNTRY,
    LOAD_TS,
    RECORD_SOURCE
FROM (
    SELECT
        {{ dv_hash(['CUSTOMER_ID'], 'HK_H_HUB_CUSTOMER') }},
        {{ dv_hash(['FIRST_NAME', 'LAST_NAME', 'CUSTOMER_EMAIL', 'CITY', 'COUNTRY'], 'HASHDIFF') }},
        FIRST_NAME,
        LAST_NAME,
        CUSTOMER_EMAIL,
        CITY,
        COUNTRY,
        CURRENT_TIMESTAMP() AS LOAD_TS,
        'RAW_CUSTOMERS' AS RECORD_SOURCE
    FROM {{ source('demo_db_dbt_dev', 'RAW_CUSTOMERS') }}
    WHERE CUSTOMER_ID IS NOT NULL
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY HK_H_HUB_CUSTOMER, HASHDIFF
    ORDER BY LOAD_TS
) = 1

{{
    config(
        materialized='dynamic_table',
        snowflake_warehouse='COMPUTE_WH',
        target_lag='1 day',
        on_configuration_change='apply'
    )
}}

{#
    HUB: HUB_CUSTOMERS
    Business Keys: CUSTOMER_ID
    Source: RAW_CUSTOMERS
#}

SELECT
    HK_H_CUSTOMERS,
    CUSTOMER_ID,
    EFFECTIVE_TS,
    DWH_VALID_TS,
    DSS_RECORD_SOURCE
FROM (
    SELECT
        {{ dv_hash(['CUSTOMER_ID'], 'HK_H_CUSTOMERS') }},
        CUSTOMER_ID,
        CURRENT_TIMESTAMP() AS EFFECTIVE_TS,
        CURRENT_TIMESTAMP() AS DWH_VALID_TS,
        'DEMO_DB.DBT_DEV.RAW_CUSTOMERS' AS DSS_RECORD_SOURCE
    FROM {{ source('raw_data', 'RAW_CUSTOMERS') }}
    WHERE CUSTOMER_ID IS NOT NULL
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY HK_H_CUSTOMERS
    ORDER BY DWH_VALID_TS
) = 1

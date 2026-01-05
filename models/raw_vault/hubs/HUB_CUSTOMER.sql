{{
    config(
        materialized='dynamic_table',
        snowflake_warehouse='COMPUTE_WH',
        target_lag='1 day',
        on_configuration_change='apply'
    )
}}

{#
    =========================================================================
    HUB: HUB_CUSTOMER (Dynamic Table)
    =========================================================================
    Business Keys: CUSTOMER_ID
#}

SELECT
    HK_H_HUB_CUSTOMER,
    CUSTOMER_ID,
    LOAD_TS,
    RECORD_SOURCE
FROM (
    SELECT
        {{ dv_hash(['CUSTOMER_ID'], 'HK_H_HUB_CUSTOMER') }},
        CUSTOMER_ID,
        CURRENT_TIMESTAMP() AS LOAD_TS,
        'RAW_CUSTOMERS' AS RECORD_SOURCE
    FROM {{ source('demo_db_dbt_dev', 'RAW_CUSTOMERS') }}
    WHERE CUSTOMER_ID IS NOT NULL
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY HK_H_HUB_CUSTOMER
    ORDER BY LOAD_TS
) = 1

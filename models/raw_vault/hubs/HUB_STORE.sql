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
    HUB: HUB_STORE (Dynamic Table)
    =========================================================================
    Business Keys: STORE_KEY
#}

SELECT
    HK_H_HUB_STORE,
    STORE_KEY,
    LOAD_TS,
    RECORD_SOURCE
FROM (
    SELECT
        {{ dv_hash(['STORE_KEY'], 'HK_H_HUB_STORE') }},
        STORE_KEY,
        CURRENT_TIMESTAMP() AS LOAD_TS,
        'RAW_ORDERS' AS RECORD_SOURCE
    FROM {{ source('demo_db_dbt_dev', 'RAW_ORDERS') }}
    WHERE STORE_KEY IS NOT NULL
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY HK_H_HUB_STORE
    ORDER BY LOAD_TS
) = 1

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
    HUB: HUB_ORDER (Dynamic Table)
    =========================================================================
    Business Keys: ORDER_ID
#}

SELECT
    HK_H_HUB_ORDER,
    ORDER_ID,
    LOAD_TS,
    RECORD_SOURCE
FROM (
    SELECT
        {{ dv_hash(['ORDER_ID'], 'HK_H_HUB_ORDER') }},
        ORDER_ID,
        CURRENT_TIMESTAMP() AS LOAD_TS,
        'RAW_ORDERS' AS RECORD_SOURCE
    FROM {{ source('demo_db_dbt_dev', 'RAW_ORDERS') }}
    WHERE ORDER_ID IS NOT NULL
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY HK_H_HUB_ORDER
    ORDER BY LOAD_TS
) = 1

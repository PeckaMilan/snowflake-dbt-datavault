{{
    config(
        materialized='dynamic_table',
        snowflake_warehouse='COMPUTE_WH',
        target_lag='1 day',
        on_configuration_change='apply'
    )
}}

{# SATELLITE: SAT_ORDER_DETAILS - Parent Hub: HUB_ORDER #}

SELECT
    HK_H_HUB_ORDER,
    HASHDIFF,
    ORDER_DATE,
        TOTAL_PRICE,
        ORDER_STATE,
    LOAD_TS,
    RECORD_SOURCE
FROM (
    SELECT
        {{ dv_hash(['ORDER_ID'], 'HK_H_HUB_ORDER') }},
        {{ dv_hash(['ORDER_DATE', 'TOTAL_PRICE', 'ORDER_STATE'], 'HASHDIFF') }},
        ORDER_DATE,
        TOTAL_PRICE,
        ORDER_STATE,
        CURRENT_TIMESTAMP() AS LOAD_TS,
        'RAW_ORDERS' AS RECORD_SOURCE
    FROM {{ source('demo_db_dbt_dev', 'RAW_ORDERS') }}
    WHERE ORDER_ID IS NOT NULL
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY HK_H_HUB_ORDER, HASHDIFF
    ORDER BY LOAD_TS
) = 1

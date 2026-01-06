{{
    config(
        materialized='dynamic_table',
        snowflake_warehouse='COMPUTE_WH',
        target_lag='1 day',
        on_configuration_change='apply'
    )
}}

{# LINK: LINK_ORDER_PRODUCT - Connecting: ORDER_ID, PRODUCT_ID #}

SELECT
    HK_L_LINK_ORDER_PRODUCT,
    HK_H_ORDER,
    HK_H_PRODUCT,
    ORDER_ID,
        PRODUCT_ID,
    LOAD_TS,
    RECORD_SOURCE
FROM (
    SELECT
        {{ dv_hash(['ORDER_ID', 'PRODUCT_ID'], 'HK_L_LINK_ORDER_PRODUCT') }},
        {{ dv_hash(['ORDER_ID'], 'HK_H_ORDER') }},
        {{ dv_hash(['PRODUCT_ID'], 'HK_H_PRODUCT') }},
        ORDER_ID,
        PRODUCT_ID,
        CURRENT_TIMESTAMP() AS LOAD_TS,
        'RAW_ORDERS' AS RECORD_SOURCE
    FROM {{ source('demo_db_dbt_dev', 'RAW_ORDERS') }}
    WHERE ORDER_ID IS NOT NULL AND PRODUCT_ID IS NOT NULL
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY HK_L_LINK_ORDER_PRODUCT
    ORDER BY LOAD_TS
) = 1

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
    HUB: HUB_PRODUCT (Dynamic Table)
    =========================================================================
    Business Keys: PRODUCT_ID
#}

SELECT
    HK_H_HUB_PRODUCT,
    PRODUCT_ID,
    LOAD_TS,
    RECORD_SOURCE
FROM (
    SELECT
        {{ dv_hash(['PRODUCT_ID'], 'HK_H_HUB_PRODUCT') }},
        PRODUCT_ID,
        CURRENT_TIMESTAMP() AS LOAD_TS,
        'RAW_PRODUCTS' AS RECORD_SOURCE
    FROM {{ source('demo_db_dbt_dev', 'RAW_PRODUCTS') }}
    WHERE PRODUCT_ID IS NOT NULL
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY HK_H_HUB_PRODUCT
    ORDER BY LOAD_TS
) = 1

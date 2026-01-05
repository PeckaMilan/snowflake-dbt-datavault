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
    HUB: HUB_SUPPLIER (Dynamic Table)
    =========================================================================
    Business Keys: SUPPLIER_ID
#}

SELECT
    HK_H_HUB_SUPPLIER,
    SUPPLIER_ID,
    LOAD_TS,
    RECORD_SOURCE
FROM (
    SELECT
        {{ dv_hash(['SUPPLIER_ID'], 'HK_H_HUB_SUPPLIER') }},
        SUPPLIER_ID,
        CURRENT_TIMESTAMP() AS LOAD_TS,
        'RAW_PRODUCTS' AS RECORD_SOURCE
    FROM {{ source('demo_db_dbt_dev', 'RAW_PRODUCTS') }}
    WHERE SUPPLIER_ID IS NOT NULL
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY HK_H_HUB_SUPPLIER
    ORDER BY LOAD_TS
) = 1

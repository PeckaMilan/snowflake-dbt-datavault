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
    LINK: LINK_PRODUCT_SUPPLIER (Dynamic Table)
    =========================================================================

    Link entity
#}

SELECT
    HK_L_LINK_PRODUCT_SUPPLIER,
    HK_H_HUB1,
    HK_H_HUB2,
    EFFECTIVE_TS,
    DWH_VALID_TS,
    DSS_RECORD_SOURCE
FROM (
    SELECT
        -- Link Hash Key
        {{ dv_hash(['KEY1', 'KEY2'], 'HK_L_LINK_PRODUCT_SUPPLIER') }},

        -- Hub Hash Keys
        HK_H_HUB1,
    HK_H_HUB2,

        -- Technical Columns
        EFFECTIVE_TS,
        DWH_VALID_TS,
        DSS_RECORD_SOURCE

    FROM {{ source('demo_db_dbt_dev', 'RAW_PRODUCTS') }}
)
-- Deduplication
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY HK_L_LINK_PRODUCT_SUPPLIER
    ORDER BY DWH_VALID_TS
) = 1

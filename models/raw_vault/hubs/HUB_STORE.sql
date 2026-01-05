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

    Hub entity

    

    Business Keys:
    - STORE_KEY
#}

SELECT
    HK_H_HUB_STORE,
    STORE_KEY,
    EFFECTIVE_TS,
    DWH_VALID_TS,
    DSS_RECORD_SOURCE
FROM (
    SELECT
        -- Hash Key (Data Vault 2.0 pattern)
        {{ dv_hash(['STORE_KEY'], 'HK_H_HUB_STORE') }},

        -- Business Keys
        STORE_KEY,

        -- Technical Columns
        EFFECTIVE_TS,
        DWH_VALID_TS,
        DSS_RECORD_SOURCE

    FROM {{ source('demo_db_dbt_dev', 'RAW_ORDERS') }}

    WHERE STORE_KEY IS NOT NULL
)
-- Deduplication (Data Vault 2.0 pattern)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY HK_H_HUB_STORE
    ORDER BY DWH_VALID_TS
) = 1

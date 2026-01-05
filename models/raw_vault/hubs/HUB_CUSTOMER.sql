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

    Hub entity

    

    Business Keys:
    - CUSTOMER_ID
#}

SELECT
    HK_H_HUB_CUSTOMER,
    CUSTOMER_ID,
    EFFECTIVE_TS,
    DWH_VALID_TS,
    DSS_RECORD_SOURCE
FROM (
    SELECT
        -- Hash Key (Data Vault 2.0 pattern)
        {{ dv_hash(['CUSTOMER_ID'], 'HK_H_HUB_CUSTOMER') }},

        -- Business Keys
        CUSTOMER_ID,

        -- Technical Columns
        EFFECTIVE_TS,
        DWH_VALID_TS,
        DSS_RECORD_SOURCE

    FROM {{ source('demo_db_dbt_dev', 'RAW_ORDERS') }}

    WHERE CUSTOMER_ID IS NOT NULL
)
-- Deduplication (Data Vault 2.0 pattern)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY HK_H_HUB_CUSTOMER
    ORDER BY DWH_VALID_TS
) = 1

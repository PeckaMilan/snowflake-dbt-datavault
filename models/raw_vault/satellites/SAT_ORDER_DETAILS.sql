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
    SATELLITE: SAT_ORDER_DETAILS (Dynamic Table)
    =========================================================================

    Satellite entity

    

    Parent Hub: N/A
#}

{%- set payload_columns = [
    'ORDER_DATE',
    'COMPLETION_DATE',
    'TOTAL_PRICE',
    'ORDER_STATE',
    'PAYMENT_STATE',
    'PAYMENT_METHOD'
] -%}

SELECT
    HK_H_SAT_ORDER_DETAILS,
    HASHDIFF,
    {% for col in payload_columns %}
    {{ col }},
    {% endfor %}
    EFFECTIVE_TS,
    DWH_VALID_TS,
    DSS_RECORD_SOURCE
FROM (
    SELECT
        -- Parent Hub Hash Key
        {{ dv_hash(['ORDER_ID', 'PRODUCT_ID'], 'HK_H_SAT_ORDER_DETAILS') }},

        -- Change Detection Hash
        {{ dv_hashdiff(payload_columns, 'HASHDIFF') }},

        -- Payload Columns
        {% for col in payload_columns %}
        {{ col }},
        {% endfor %}

        -- Technical Columns
        EFFECTIVE_TS,
        DWH_VALID_TS,
        DSS_RECORD_SOURCE

    FROM {{ source('demo_db_dbt_dev', 'RAW_ORDERS') }}
)
-- Deduplication by hash key + hashdiff (first occurrence)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY HK_H_SAT_ORDER_DETAILS, HASHDIFF
    ORDER BY DWH_VALID_TS
) = 1

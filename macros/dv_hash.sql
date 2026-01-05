{#
    Data Vault 2.0 Hash Macro
    Generates MD5 hash key from business key columns
    WhereScape compatible: pipe-separated, COALESCE for NULLs
#}

{% macro dv_hash(columns, alias) %}
    {%- set algorithm = var('hash_algorithm', 'MD5') -%}
    {{ algorithm }}(
        {%- for column in columns %}
        COALESCE(CAST({{ column }} AS VARCHAR), '')
        {%- if not loop.last %} || '|' || {% endif %}
        {%- endfor %}
    ) AS {{ alias }}
{% endmacro %}


{% macro dv_hashdiff(columns, alias) %}
    {%- set algorithm = var('hash_algorithm', 'MD5') -%}
    {{ algorithm }}(
        {%- for column in columns %}
        COALESCE(CAST({{ column }} AS VARCHAR), '')
        {%- if not loop.last %} || '|' || {% endif %}
        {%- endfor %}
    ) AS {{ alias }}
{% endmacro %}

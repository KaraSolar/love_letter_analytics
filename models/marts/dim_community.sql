{{
    config(
        materialized='incremental',
        unique_key='community_id',
        schema='marts',
        on_schema_change='fail'
    )
}}

SELECT
    community_id
    ,name
    ,ST_GEOGPOINT(
        SAFE_CAST(SUBSTR(coordinates, INSTR(coordinates, ',') + 1) AS NUMERIC), -- longitude
        SAFE_CAST(SUBSTR(coordinates, 1, INSTR(coordinates, ',') - 1) AS NUMERIC) -- latitude
        ) AS coordinates
    ,region
    ,population
    ,households
    ,healthcare_availability
    ,is_active
    ,updated_at
FROM {{ ref('community')}}
{% if is_incremental() %}
WHERE updated_at >= (SELECT COALESCE(MAX(updated_at),'1900-01-01') FROM {{ this }} )
{% endif %}

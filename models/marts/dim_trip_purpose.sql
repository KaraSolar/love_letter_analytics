{{
    config(
        materialized='incremental',
        unique_key='trip_purpose_id',
        schema='marts',
        on_schema_change='fail'
    )
}}

SELECT
    trip_purpose_id
    ,purpose
    ,is_critical
    ,description
    ,priority_level
    ,is_emergency_trip
    ,updated_at
FROM {{ ref('trip_purposes')}}
{% if is_incremental() %}
WHERE updated_at > (SELECT COALESCE(MAX(updated_at),'1900-01-01') FROM {{ this }} )
{% endif %}

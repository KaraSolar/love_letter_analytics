{{
    config(
        materialized='incremental',
        unique_key='boat_id',
        schema='marts',
        on_schema_change='fail'
    )
}}

SELECT
    boat_id
    ,name
    ,main_community
    ,cerbo_gx_model
    ,gps_model
    ,passenger_capacity
    ,captain
    ,updated_at
    ,is_active
    ,boat_type
    ,battery_capacity
FROM {{ ref('boats')}}
{% if is_incremental() %}
WHERE updated_at >= (SELECT COALESCE(MAX(updated_at),'1900-01-01') FROM {{ this }} )
{% endif %}

{{
    config(
        materialized='incremental',
        unique_key='date_id',
        schema='marts',
        on_schema_change='fail'
    )
}}

WITH date_series AS (
    SELECT
        date
    FROM
        UNNEST(GENERATE_DATE_ARRAY('2024-07-01', '2050-01-01', INTERVAL 1 DAY)) AS date
)
SELECT
    CAST(REPLACE(CAST(date AS STRING),'-','') AS INT64) AS date_id
    ,date
    ,EXTRACT(DAYOFWEEK FROM date) as day_of_week_number
    ,FORMAT_DATE('%A', date) as day_of_week_text
    ,FORMAT_DATE('%d', date) as day_number_in_month
    ,EXTRACT(WEEK FROM date) as week_number_in_year
    ,EXTRACT(MONTH FROM date) as month_number
    ,FORMAT_DATE('%B', date) as month_text
    ,EXTRACT(QUARTER FROM date) as quarter
    ,EXTRACT(YEAR FROM date) as year
    ,EXTRACT(DAYOFWEEK FROM date) IN (1,7) AS is_weekend
FROM date_series
{% if is_incremental() %}
WHERE date > (SELECT COALESCE(MAX(date),'1900-01-01') FROM {{ this }} )
{% endif %}
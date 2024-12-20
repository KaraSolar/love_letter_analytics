{{
    config(
        materialized='view',
        schema='transformation',
    )
}}

WITH trip_order AS (
    SELECT
        boat
        ,telemetry_datetime
        ,DENSE_RANK() OVER(PARTITION BY boat, date_trunc(telemetry_datetime, DAY)
                            ORDER BY trip_id) AS trip_number
        ,ST_GEOGPOINT(longitude, latitude) AS coordinates
        ,battery_power
        ,pvdc_coupled_power
        ,speed
        ,passenger_quantity
        ,trip_purpose
    FROM {{ ref('stg_telemetry')}}
    WHERE trip_id IS NOT NULL
)
, t_distance AS (
  SELECT
    *
    ,ST_DISTANCE(coordinates,
                LAG(coordinates) OVER(PARTITION BY trip_number
                                    ORDER BY telemetry_datetime)) AS distance
    ,ROW_NUMBER() OVER(PARTITION BY trip_number
                        ORDER BY telemetry_datetime) AS points_order_asc
    ,ROW_NUMBER() OVER(PARTITION BY trip_number
                        ORDER BY telemetry_datetime DESC) AS points_order_desc
  from trip_order
)

SELECT
    t_distance.trip_number
    ,{{ dbt_utils.generate_surrogate_key(['boat', 'MIN(telemetry_datetime)', 'MAX(telemetry_datetime)']) }} AS trip_id
    ,boat
    ,MIN(telemetry_datetime) AS trip_start
    ,ANY_VALUE(trip_purpose) AS trip_purpose
    ,MAX(telemetry_datetime) AS trip_end
    ,ST_MAKELINE(ARRAY_AGG(coordinates IGNORE NULLS
                        ORDER BY telemetry_datetime)) AS trip_linestring
    ,ARRAY_AGG(IF(points_order_asc <= 100, coordinates, NULL) IGNORE NULLS) AS first_hundred_points
    ,ARRAY_AGG(IF(points_order_desc <= 100, coordinates, NULL) IGNORE NULLS) AS last_hundred_points
    ,ANY_VALUE(passenger_quantity) AS number_of_passengers
    ,SUM(distance) AS distance_meters
    ,DATETIME_DIFF(MAX(telemetry_datetime),
                    MIN(telemetry_datetime), SECOND) AS trip_duration_seconds
    ,SUM(IF(battery_power < 0, battery_power, NULL)) AS power_consumed_battery
    ,SUM(IF(pvdc_coupled_power < 0, pvdc_coupled_power, NULL)) AS power_consumed_solar_panels
    ,SUM(IF(pvdc_coupled_power > 0, pvdc_coupled_power, NULL)) AS power_generated_solar_panels
    ,AVG(speed) AS avg_speed
    ,MIN(speed) AS min_speed
    ,MAX(speed) AS max_speed
FROM t_distance
GROUP BY date_trunc(telemetry_datetime, DAY), trip_number, boat


{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        schema='marts',
        on_schema_change='fail'
    )
}}

SELECT
    t.trip_id
    ,b.boat_id
    ,trip_start
    ,dd1.date_id AS trip_start_date_id
    ,departure.community_id AS community_departure_id
    ,arrival.community_id AS community_arrival_id
    ,p.trip_purpose_id
    ,trip_end
    ,dd2.date_id AS trip_end_date_id
    ,trip_linestring
    ,number_of_passengers
    ,distance_meters * 0.001 AS distance_km
    ,distance_meters * 0.000621371 AS distance_mi
    ,distance_meters * 0.000539957 AS distance_nmi
    ,trip_duration_seconds / 60 AS trip_duration_minutes
    ,trip_duration_seconds
    ,trip_duration_seconds / 3600 AS trip_duration_hours
    ,power_consumed_battery + power_consumed_solar_panels AS total_energy_consumed
    ,power_consumed_battery
    ,power_consumed_solar_panels
    ,power_generated_solar_panels
    ,avg_speed
    ,min_speed
    ,max_speed
FROM {{ ref('int_trips')}} t
    INNER JOIN {{ ref('dim_boat')}} b
    ON t.boat = b.name
    LEFT JOIN {{ ref('dim_trip_purpose')}} p
    ON t.trip_purpose = p.purpose
    INNER JOIN {{ ref('dim_date')}} dd1
    ON DATE_TRUNC(trip_start, DAY) = dd1.date
    INNER JOIN {{ ref('dim_date')}} dd2
    ON DATE_TRUNC(trip_end, DAY) = dd2.date
    LEFT JOIN (
                SELECT trip_id, any_value(community_id) AS community_id
                FROM {{ ref('int_trips') }}
                ,UNNEST(first_hundred_points) AS fh
                 INNER JOIN {{ ref('dim_community')}} dc
                    ON ST_WITHIN(fh,dc.community_polygon)
                WHERE ARRAY_LENGTH(first_hundred_points) > 1
                GROUP BY trip_id
             ) departure
    ON t.trip_id = departure.trip_id
    LEFT JOIN (
                SELECT trip_id, any_value(community_id) as community_id
                FROM {{ ref('int_trips') }}
                ,UNNEST(last_hundred_points) AS lh
                INNER JOIN {{ ref('dim_community') }} dc
                    ON ST_WITHIN(lh,dc.community_polygon)
                WHERE ARRAY_LENGTH(last_hundred_points) > 1
                GROUP BY trip_id
            ) arrival
    ON t.trip_id = arrival.trip_id
{% if is_incremental() %} -- logs are immutable, there wont be any updates
WHERE t.trip_id NOT IN (SELECT trip_id FROM {{ this }} )
{% endif %}

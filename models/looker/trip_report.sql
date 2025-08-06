
SELECT
    -- #####    FACT TRIP
    t.trip_id
    ,t.trip_start
    ,t.trip_end
    ,t.trip_linestring
    ,t.number_of_passengers
    ,t.distance_km
    ,t.distance_mi
    ,t.distance_nmi
    ,t.trip_duration_minutes
    ,t.trip_duration_seconds
    ,t.trip_duration_hours
    ,t.total_energy_consumed
    ,t.power_consumed_battery
    ,t.power_consumed_solar_panels
    ,t.power_generated_solar_panels
    ,t.avg_speed
    ,t.min_speed
    ,t.max_speed
    ,t.file_date


    -- #####    BOAT
    ,b.name AS boat_name
    ,b.main_community
    ,b.cerbo_gx_model
    ,b.gps_model
    ,b.passenger_capacity
    ,b.captain
    ,b.is_active
    ,b.boat_type
    ,b.battery_capacity

    -- #####    DATE START
    ,dd1.date AS trip_start_date

    -- #####    COMMUNITY DEPARTURE
    ,dc1.name AS departure_community
    ,dc1.coordinates AS departure_community_coordinates
    ,dc1.region AS departure_community_region
    ,dc1.population AS departure_community_population
    ,dc1.households AS departure_community_households
    ,dc1.healthcare_availability AS departure_community_healthcare_availability
    ,dc1.is_active AS departure_community_is_active
    ,dc1.community_polygon AS departure_community_polygon

    -- #####    COMMUNITY ARRIVAL
    ,dc2.name AS arrival_community
    ,dc2.coordinates AS arrival_community_coordinates
    ,dc2.region AS arrival_community_region
    ,dc2.population AS arrival_community_population
    ,dc2.households AS arrival_community_households
    ,dc2.healthcare_availability AS arrival_community_healthcare_availability
    ,dc2.is_active AS arrival_community_is_active
    ,dc2.community_polygon AS arrival_community_polygon

    -- #####    TRIP PURPOSE
    ,p.purpose
    ,p.is_critical
    ,p.description
    ,p.priority_level
    ,p.is_emergency_trip

    -- #####    DATE END
    ,dd2.date AS trip_end_date

    -- #####    TRIP DISCRIMINATION
    ,CASE
        WHEN b.name = 'taller' THEN True
        WHEN t.file_date < '2025-06-28' THEN True
        ELSE False
    END AS is_test_trip
FROM {{ ref('fact_trip')}} t
    INNER JOIN {{ ref('dim_boat')}} b
    ON t.boat_id = b.boat_id
    LEFT JOIN {{ ref('dim_trip_purpose')}} p
    ON t.trip_purpose_id = p.trip_purpose_id
    INNER JOIN {{ ref('dim_date')}} dd1
    ON t.trip_start_date_id = dd1.date_id
    INNER JOIN {{ ref('dim_date')}} dd2
    ON t.trip_end_date_id = dd2.date_id
    LEFT JOIN {{ ref('dim_community')}} dc1
    ON t.community_departure_id = dc1.community_id
    LEFT JOIN {{ ref('dim_community')}} dc2
    ON t.community_arrival_id = dc2.community_id

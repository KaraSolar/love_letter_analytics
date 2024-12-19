{{ config(materialized='view') }}

WITH casted_datatypes AS (

    SELECT
        Boat AS boat
        ,SAFE_CAST(telemetryId AS NUMERIC) AS telemetry_id
        ,parse_datetime('%F %T',telemetryTimeStamp) AS telemetry_datetime
        ,SAFE_CAST(tripId AS NUMERIC) AS trip_id
        ,SAFE_CAST(telemetryBatteryVoltageSystem AS NUMERIC) AS battery_voltage
        ,SAFE_CAST(telemetryBatteryCurrentSystem AS NUMERIC) AS battery_current
        ,SAFE_CAST(telemetryBatteryPowerSystem AS NUMERIC) AS battery_power
        ,SAFE_CAST(telemetryBatteryStateOfChargeSystem AS NUMERIC) AS battery_state_of_charge
        ,SAFE_CAST(telemetryPVDCCoupledPower AS NUMERIC) AS pvdc_coupled_power
        ,SAFE_CAST(telemetryPVDCCoupledCurrent AS NUMERIC) AS pdvc_coupled_current
        ,SAFE_CAST(telemetryLatitude1 AS INT64) AS latitude_1
        ,SAFE_CAST(telemetryLatitude2 AS INT64) AS latitude_2
        ,SAFE_CAST(telemetryLongitude1 AS INT64) AS longitude_1
        ,SAFE_CAST(telemetryLongitude2 AS INT64) AS longitude_2
        ,SAFE_CAST(telemetryCourse AS NUMERIC) AS course
        ,SAFE_CAST(telemetrySpeed AS NUMERIC) AS speed
        ,SAFE_CAST(telemetryGPSFix AS NUMERIC) AS gps_fix
        ,SAFE_CAST(telemetryGPSNumberOfSatellites AS NUMERIC) AS number_of_satellites
        ,SAFE_CAST(telemetryAltitude1 AS INT64) AS altitude_1
        ,SAFE_CAST(telemetryAltitude2 AS INT64) AS altitude_2
        ,SAFE_CAST(tripPassengerQty AS NUMERIC) AS passenger_quantity
        ,tripPurpose AS trip_purpose
    FROM {{source('loveletter_raw','telemetry')}}

)

SELECT
    boat
    ,telemetry_id
    ,telemetry_datetime
    ,trip_id
    ,battery_voltage
    ,battery_current
    ,battery_power
    ,battery_state_of_charge
    ,pvdc_coupled_power
    ,pdvc_coupled_current
    ,{{ registers_decoder('latitude_1', 'latitude_2', 10000000)}} AS latitude
    ,{{ registers_decoder('longitude_1', 'longitude_2', 10000000) }} AS longitude
    ,course
    ,speed
    ,gps_fix
    ,number_of_satellites
    ,{{ registers_decoder('altitude_1', 'altitude_2', 10) }} AS altitude
    ,passenger_quantity
    ,trip_purpose
FROM casted_datatypes

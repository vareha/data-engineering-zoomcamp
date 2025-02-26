{{ config(materialized='table') }}

with dim_fhv_trips as (
    select
        tripid,
        pickup_locationid,
        pickup_borough,
        pickup_zone,
        dropoff_locationid,
        dropoff_borough,
        dropoff_zone,
        year,
        month,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration
    from {{ ref('dim_fhv_trips') }}
)
    select
       year,
       month,
       pickup_zone,
       dropoff_zone,
       APPROX_QUANTILES(trip_duration, 100)[OFFSET(90)] AS p90_trip_duration_seconds
       FROM dim_fhv_trips
       GROUP BY year, month, pickup_zone, dropoff_zone

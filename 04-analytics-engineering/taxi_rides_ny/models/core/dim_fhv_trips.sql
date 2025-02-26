{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_fhv_trips') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
    select 
        tripid,
        pickup_locationid,
        pickup_datetime,
        pickup_zone.borough as pickup_borough, 
        pickup_zone.zone as pickup_zone,
        dropoff_locationid, 
        dropoff_datetime,
        dropoff_zone.borough as dropoff_borough, 
        dropoff_zone.zone as dropoff_zone,  
        dispatching_base_num,
        affiliated_base_num,
        sr_flag,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        from trips_data
        join dim_zones as pickup_zone
        on trips_data.pickup_locationid = pickup_zone.locationid
        join dim_zones as dropoff_zone
        on trips_data.dropoff_locationid = dropoff_zone.locationid
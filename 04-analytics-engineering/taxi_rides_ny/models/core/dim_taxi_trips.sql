{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
)
    select 
        tripid,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
        CONCAT(EXTRACT(YEAR FROM pickup_datetime), '/Q', EXTRACT(QUARTER FROM pickup_datetime)) AS year_quarter,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        pickup_datetime
        from trips_data
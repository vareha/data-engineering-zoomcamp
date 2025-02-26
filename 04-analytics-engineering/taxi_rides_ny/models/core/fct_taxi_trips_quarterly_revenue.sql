{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
),
dim_taxi_trips  as (
    select * from {{ ref("dim_taxi_trips")}}
)

    select
        dim_taxi_trips.year,
        dim_taxi_trips.quarter,
        dim_taxi_trips.year_quarter,
        -- dim_taxi_trips.month,
        service_type,
        SUM(trips_data.total_amount) AS total_revenue
        -- (AS year_to_year
        from trips_data
        join dim_taxi_trips
        on trips_data.tripid = dim_taxi_trips.tripid
        where dim_taxi_trips.year in (2020, 2019)
        group by
        dim_taxi_trips.year,
        dim_taxi_trips.quarter,
        dim_taxi_trips.year_quarter,
        -- dim_taxi_trips.month,
        trips_data.service_type
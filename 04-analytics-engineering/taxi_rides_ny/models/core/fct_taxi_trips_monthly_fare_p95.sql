{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
),
dim_taxi_trips  as (
    select * from {{ ref("dim_taxi_trips")}}
)

    select
        dim_taxi_trips.year,
        dim_taxi_trips.month,
        service_type,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(90)] AS percentile_90,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(95)] AS percentile_95,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(97)] AS percentile_97
        from trips_data
        join dim_taxi_trips
        on trips_data.tripid = dim_taxi_trips.tripid
        where dim_taxi_trips.year in (2020, 2019)
        group by
        dim_taxi_trips.year,
        dim_taxi_trips.month,
        trips_data.service_type
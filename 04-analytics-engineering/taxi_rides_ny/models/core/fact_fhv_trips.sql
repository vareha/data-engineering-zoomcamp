{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
) 
select fhv_tripdata.tripid, 
        fhv_tripdata.pickup_locationid,
        fhv_tripdata.dropoff_locationid,
        fhv_tripdata.pickup_datetime,
        fhv_tripdata.dropoff_datetime,
        fhv_tripdata.dispatching_base_num,
        fhv_tripdata.affiliated_base_num,
        fhv_tripdata.sr_flag
from fhv_tripdata
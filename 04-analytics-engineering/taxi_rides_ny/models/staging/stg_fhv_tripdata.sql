-- with 

-- source as (

--     select * from {{ source('staging', 'fhv_tripdata') }}

-- ),

-- renamed as (

--     select
--         unique_row_id,
--         filename,
--         dispatching_base_num,
--         pickup_datetime,
--         dropoff_datetime,
--         pulocationid,
--         dolocationid,
--         sr_flag,
--         affiliated_base_number

--     from source

-- )

-- select * from renamed

{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *,
  from {{ source('staging','fhv_tripdata') }}
  where dispatching_base_num is not null 
)
select
    -- {{ dbt.safe_cast("unique_row_id", api.Column.translate_type("string")) }} as tripid,
    TO_HEX(unique_row_id) AS tripid,
    {{ dbt.safe_cast("PULocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    {{ dbt.safe_cast("dispatching_base_num", api.Column.translate_type("string")) }} as dispatching_base_num,
    {{ dbt.safe_cast("Affiliated_base_number", api.Column.translate_type("string")) }} as affiliated_base_num,
    {{ dbt.safe_cast("SR_Flag", api.Column.translate_type("integer")) }} as sr_flag

from tripdata
-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
-- {% if var('is_test_run', default=true) %}

--   limit 100

-- {% endif %}
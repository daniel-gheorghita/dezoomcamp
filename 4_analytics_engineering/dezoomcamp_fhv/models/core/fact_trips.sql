{{ config(materialized='table') }}

with fhv_trips as (
    select * 
    from {{ ref('stg_fhv_tripdata') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
--    fhv_trips.tripid, 
--    fhv_trips.vendorid, 
    fhv_trips.dispatching_base_num,
--    fhv_trips.service_type,
--    fhv_trips.ratecodeid, 
    fhv_trips.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_trips.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_trips.pickup_datetime, 
    fhv_trips.dropoff_datetime
--    trips_unioned.store_and_fwd_flag, 
--    trips_unioned.passenger_count, 
--    trips_unioned.trip_distance, 
--    trips_unioned.trip_type, 
--    trips_unioned.fare_amount, 
--    trips_unioned.extra, 
--    trips_unioned.mta_tax, 
--    trips_unioned.tip_amount, 
--    trips_unioned.tolls_amount, 
--    trips_unioned.ehail_fee, 
--    trips_unioned.improvement_surcharge, 
--    trips_unioned.total_amount, 
--    trips_unioned.payment_type, 
--    trips_unioned.payment_type_description, 
--    trips_unioned.congestion_surcharge
from fhv_trips
inner join dim_zones as pickup_zone
on fhv_trips.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_trips.dropoff_locationid = dropoff_zone.locationid
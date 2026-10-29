-- Trusted repository template for notebook 41.
-- job_taxi_asset is the session view created from samples.nyctaxi.trips.
-- Execute with spark.sql(template, args={'run_date': parsed_date}).
SELECT trip_id, pickup_ts, dropoff_ts, pickup_zip, dropoff_zip, distance, fare
FROM job_taxi_asset
WHERE pickup_date = :run_date

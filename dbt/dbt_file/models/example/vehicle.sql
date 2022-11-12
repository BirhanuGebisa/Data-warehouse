{{ config(materialized='view') }}

with summary as (
  
    SELECT 
    type as "Vehicle type",
    count(type) as "vehicle count",
    Round(AVG(Cast(traveled_d as numeric)),2) as "Avg distance",
    Round(AVG(cast(avg_speed as numeric)),2) as "Avg speed"
    from trajectories 
    GROUP BY type ORDER BY "vehicle count" ASC
  
)

SELECT * from summary
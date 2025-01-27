With base_cte as (
select 
	case 
		WHEN trip_distance <= 1 THEN 'Up to 1 mile'
        WHEN trip_distance > 1 AND trip_distance <= 3 THEN '1 to 3 miles'
        WHEN trip_distance > 3 AND trip_distance <= 7 THEN '3 to 7 miles'
        WHEN trip_distance > 7 AND trip_distance <= 10 THEN '7 to 10 miles'
        ELSE 'Over 10 miles'
	end as trip_distance_group, trip_distance
from green_taxi_data 
where date(lpep_pickup_datetime) between '2019-10-01' and '2019-10-31'
--and lpep_dropoff_datetime > lpep_pickup_datetime
--and trip_distance > 0 
)
select  trip_distance_group, count(trip_distance)
from base_cte
group by trip_distance_group
order by trip_distance_group
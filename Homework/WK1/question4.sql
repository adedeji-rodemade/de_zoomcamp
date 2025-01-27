select 	
	date(lpep_pickup_datetime) as pickup_date, 
	MAX(trip_distance) as longest_distance
from green_taxi_data 
group by pickup_date
order by longest_distance desc
limit 1
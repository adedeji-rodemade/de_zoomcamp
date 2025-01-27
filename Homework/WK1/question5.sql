select 	
	z.zone, 
	sum(total_amount) as total_amount
from green_taxi_data g
left join taxi_zone_lookup z on g.pulocationid = z.locationid
WHERE lpep_pickup_datetime >= '2019-10-18 00:00:00' 
  AND lpep_pickup_datetime < '2019-10-19 00:00:00'
group by z.zone
having sum(total_amount) > 13000
order by total_amount desc
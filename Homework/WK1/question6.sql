SELECT DISTINCT
    t.zone AS dropoff_zone
FROM green_taxi_data g
LEFT JOIN taxi_zone_lookup t ON g.dolocationid = t.locationid
WHERE g.tip_amount = (
      SELECT MAX(tip_amount)
      FROM green_taxi_data
      WHERE lpep_pickup_datetime >= '2019-10-01 00:00:00'
        AND lpep_pickup_datetime < '2019-11-01 00:00:00'
        AND pulocationid = (
            SELECT locationid
            FROM taxi_zone_lookup
            WHERE zone = 'East Harlem North'
        )
  );
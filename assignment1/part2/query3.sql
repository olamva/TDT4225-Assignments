USE porto;
SELECT taxi_id, COUNT(trip_id) AS trip_count
FROM trip_by_taxi
GROUP BY taxi_id
ORDER BY trip_count DESC
LIMIT 20;
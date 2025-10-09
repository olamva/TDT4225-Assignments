USE porto;
SELECT AVG(trip_count) AS average_trips_per_taxi
FROM (
    SELECT taxi_id, COUNT(trip_id) AS trip_count
    FROM trip_by_taxi
    GROUP BY taxi_id
) AS taxi_trip_counts;
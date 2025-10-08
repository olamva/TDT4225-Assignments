USE porto;
SELECT
    t.trip_id,
    t.taxi_id,
    j.timestamp_ AS start_time,
    DATE_ADD(j.timestamp_, INTERVAL (JSON_LENGTH(j.polyline) * 15) SECOND) AS estimated_end_time
FROM trip_by_taxi t
JOIN trip_journey j ON t.trip_id = j.trip_id
WHERE j.polyline IS NOT NULL
  AND j.timestamp_ IS NOT NULL
  AND DATE(j.timestamp_) != DATE(DATE_ADD(j.timestamp_, INTERVAL (JSON_LENGTH(j.polyline) * 15) SECOND))
ORDER BY j.timestamp_;
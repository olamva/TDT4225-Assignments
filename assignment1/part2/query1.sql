USE porto;
SELECT
  (SELECT COUNT(DISTINCT taxi_id) FROM trip_by_taxi) AS Number_of_taxis,
  (SELECT COUNT(DISTINCT trip_id) FROM trip_by_taxi) AS Number_of_trips,
  (SELECT COALESCE(SUM(JSON_LENGTH(polyline)), 0) FROM trip_journey
     WHERE polyline IS NOT NULL) AS Number_of_GPS_points;
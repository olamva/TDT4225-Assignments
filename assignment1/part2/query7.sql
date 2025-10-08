USE porto;
SELECT COUNT(*) AS invalid_trips
FROM trip_journey
WHERE JSON_LENGTH(polyline) < 3 OR polyline IS NULL;
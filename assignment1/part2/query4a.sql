USE porto;
SELECT
    taxi_id,
    call_type AS most_used_call_type,
    call_count
FROM (
    SELECT
        taxi_id,
        call_type,
        COUNT(*) AS call_count,
        ROW_NUMBER() OVER (PARTITION BY taxi_id ORDER BY COUNT(*) DESC) as rn
    FROM trip_by_taxi
    GROUP BY taxi_id, call_type
) ranked
WHERE rn = 1
ORDER BY taxi_id;
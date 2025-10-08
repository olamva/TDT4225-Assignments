from datetime import datetime

import mysql.connector

# Query 11: For each taxi, compute the average idle time between consecutive trips
# List the top 20 taxis with the highest average idle time

def query11():
    conn = mysql.connector.connect(
        host="127.0.0.1", port=3306, user="root", password="secret", database="porto"
    )

    # Get all trips with their timestamps, ordered by taxi and time
    sql = """
    SELECT
        t.taxi_id,
        t.trip_id,
        j.timestamp_,
        JSON_LENGTH(j.polyline) * 15 AS estimated_duration_seconds
    FROM trip_by_taxi t
    JOIN trip_journey j ON t.trip_id = j.trip_id
    WHERE j.timestamp_ IS NOT NULL AND j.polyline IS NOT NULL
    ORDER BY t.taxi_id, j.timestamp_
    """

    cur = conn.cursor()
    cur.execute(sql)

    taxi_trips = {}

    # Group trips by taxi
    for taxi_id, trip_id, timestamp, duration in cur:
        if taxi_id not in taxi_trips:
            taxi_trips[taxi_id] = []

        end_time = timestamp.timestamp() + duration
        taxi_trips[taxi_id].append({
            'trip_id': trip_id,
            'start_time': timestamp.timestamp(),
            'end_time': end_time
        })

    cur.close()
    conn.close()

    # Calculate idle times for each taxi
    taxi_idle_stats = {}

    for taxi_id, trips in taxi_trips.items():
        if len(trips) < 2:  # Need at least 2 trips to calculate idle time
            continue

        idle_times = []

        for i in range(len(trips) - 1):
            current_trip_end = trips[i]['end_time']
            next_trip_start = trips[i + 1]['start_time']

            idle_time = next_trip_start - current_trip_end

            # Only consider positive idle times (negative would mean overlapping trips)
            if idle_time > 0:
                idle_times.append(idle_time)

        if idle_times:
            avg_idle_time = sum(idle_times) / len(idle_times)
            taxi_idle_stats[taxi_id] = {
                'avg_idle_seconds': avg_idle_time,
                'avg_idle_hours': avg_idle_time / 3600,
                'num_idle_periods': len(idle_times),
                'total_trips': len(trips)
            }

    # Sort by average idle time (descending)
    sorted_taxis = sorted(taxi_idle_stats.items(), key=lambda x: x[1]['avg_idle_seconds'], reverse=True)

    return sorted_taxis

if __name__ == "__main__":
    results = query11()
    print("Top 20 taxis with highest average idle time between trips:")
    print("Taxi ID | Avg Idle Time (hours) | Idle Periods | Total Trips")
    print("-" * 65)
    for taxi_id, stats in results[:20]:
        print(f"{taxi_id:7} | {stats['avg_idle_hours']:18.2f} | {stats['num_idle_periods']:12} | {stats['total_trips']:11}")

    if len(results) > 20:
        print(f"\n... and {len(results) - 20} more taxis")
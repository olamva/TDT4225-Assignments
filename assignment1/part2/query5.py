import json
from datetime import datetime

import mysql.connector

# Query 5: Find taxis with most total hours driven and total distance driven
# List them in order of total hours

def calculate_distance(polyline):
    """Calculate distance using Haversine formula"""
    if not polyline or len(polyline) < 2:
        return 0

    import math

    def haversine(lat1, lon1, lat2, lon2):
        R = 6371000  # Earth radius in meters
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)

        a = math.sin(delta_phi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

        return R * c

    total_distance = 0
    for i in range(len(polyline) - 1):
        lon1, lat1 = polyline[i]
        lon2, lat2 = polyline[i + 1]
        total_distance += haversine(lat1, lon1, lat2, lon2)

    return total_distance

def query5():
    conn = mysql.connector.connect(
        host="127.0.0.1", port=3306, user="root", password="secret", database="porto"
    )

    sql = """
    SELECT
        t.taxi_id,
        j.polyline
    FROM trip_by_taxi t
    JOIN trip_journey j ON t.trip_id = j.trip_id
    WHERE j.polyline IS NOT NULL
    """

    cur = conn.cursor()
    cur.execute(sql)

    taxi_stats = {}

    for taxi_id, polyline_json in cur:
        if taxi_id not in taxi_stats:
            taxi_stats[taxi_id] = {'total_hours': 0, 'total_distance': 0}

        try:
            polyline = json.loads(polyline_json) if isinstance(polyline_json, str) else polyline_json

            # Calculate duration (assume 15 seconds between GPS points)
            duration_seconds = len(polyline) * 15 if polyline else 0
            duration_hours = duration_seconds / 3600

            # Calculate distance
            distance = calculate_distance(polyline)

            taxi_stats[taxi_id]['total_hours'] += duration_hours
            taxi_stats[taxi_id]['total_distance'] += distance

        except (json.JSONDecodeError, TypeError):
            continue

    cur.close()
    conn.close()

    # Sort by total hours (descending)
    sorted_taxis = sorted(taxi_stats.items(), key=lambda x: x[1]['total_hours'], reverse=True)

    return sorted_taxis

if __name__ == "__main__":
    results = query5()
    print("Top taxis by total hours driven:")
    print("Taxi ID | Total Hours | Total Distance (km)")
    print("-" * 45)
    for taxi_id, stats in results[:20]:  # Top 20
        print(f"{taxi_id:7} | {stats['total_hours']:10.2f} | {stats['total_distance']/1000:14.2f}")
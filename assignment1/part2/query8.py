import json
import math
from datetime import datetime

import mysql.connector

# Query 8: Find pairs of different taxis that were within 5m and within 5 seconds
# of each other at least once

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two points using Haversine formula"""
    R = 6371000  # Earth radius in meters
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

    return R * c

def query8():
    conn = mysql.connector.connect(
        host="127.0.0.1", port=3306, user="root", password="secret", database="porto"
    )

    # Get all trips with their polylines and timestamps
    sql = """
    SELECT
        t.taxi_id,
        t.trip_id,
        j.polyline,
        j.timestamp_
    FROM trip_by_taxi t
    JOIN trip_journey j ON t.trip_id = j.trip_id
    WHERE j.polyline IS NOT NULL AND j.timestamp_ IS NOT NULL
    ORDER BY j.timestamp_
    """

    cur = conn.cursor()
    cur.execute(sql)

    # Process trips and expand GPS points with timestamps
    gps_points = []  # [(taxi_id, trip_id, lat, lon, timestamp), ...]

    for taxi_id, trip_id, polyline_json, start_timestamp in cur:
        try:
            polyline = json.loads(polyline_json) if isinstance(polyline_json, str) else polyline_json

            if not polyline:
                continue

            # Assume 15 seconds between GPS points
            for i, point in enumerate(polyline):
                if len(point) >= 2:
                    lon, lat = point[0], point[1]
                    # Calculate timestamp for this GPS point
                    point_timestamp = start_timestamp.timestamp() + (i * 15)
                    gps_points.append((taxi_id, trip_id, lat, lon, point_timestamp))

        except (json.JSONDecodeError, TypeError, IndexError, AttributeError):
            continue

    cur.close()
    conn.close()

    # Sort by timestamp for efficient comparison
    gps_points.sort(key=lambda x: x[4])

    close_pairs = set()
    max_time_diff = 5  # 5 seconds
    max_distance = 5   # 5 meters

    print(f"Processing {len(gps_points)} GPS points...")

    # Compare points within time window
    for i in range(len(gps_points)):
        taxi1, trip1, lat1, lon1, time1 = gps_points[i]

        # Only check points within 5 seconds
        j = i + 1
        while j < len(gps_points) and gps_points[j][4] - time1 <= max_time_diff:
            taxi2, trip2, lat2, lon2, time2 = gps_points[j]

            # Skip same taxi
            if taxi1 != taxi2:
                distance = haversine_distance(lat1, lon1, lat2, lon2)
                if distance <= max_distance:
                    # Add pair (ensure consistent ordering)
                    pair = tuple(sorted([taxi1, taxi2]))
                    close_pairs.add(pair)

            j += 1

        # Progress indicator
        if i % 10000 == 0:
            print(f"Processed {i}/{len(gps_points)} points...")

    return list(close_pairs)

if __name__ == "__main__":
    results = query8()
    print(f"\nFound {len(results)} pairs of taxis that were within 5m and 5 seconds of each other:")
    for pair in results[:20]:  # Show first 20 pairs
        print(f"  Taxi {pair[0]} and Taxi {pair[1]}")
    if len(results) > 20:
        print(f"  ... and {len(results) - 20} more pairs")
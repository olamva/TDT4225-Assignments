import json
import math

import mysql.connector

# Query 10: Find trips whose start and end points are within 50m of each other (circular trips)

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

def query10():
    conn = mysql.connector.connect(
        host="127.0.0.1", port=3306, user="root", password="secret", database="porto"
    )

    sql = """
    SELECT trip_id, polyline
    FROM all_taxi_info
    WHERE polyline IS NOT NULL
    """

    cur = conn.cursor()
    cur.execute(sql)

    circular_trips = []
    max_distance = 50  # 50 meters

    for trip_id, polyline_json in cur:
        try:
            polyline = json.loads(polyline_json) if isinstance(polyline_json, str) else polyline_json

            if not polyline or len(polyline) < 2:
                continue

            # Get start and end points
            start_point = polyline[0]
            end_point = polyline[-1]

            if len(start_point) >= 2 and len(end_point) >= 2:
                start_lon, start_lat = start_point[0], start_point[1]
                end_lon, end_lat = end_point[0], end_point[1]

                distance = haversine_distance(start_lat, start_lon, end_lat, end_lon)

                if distance <= max_distance:
                    circular_trips.append({
                        'trip_id': trip_id,
                        'start_end_distance': distance,
                        'start_point': (start_lat, start_lon),
                        'end_point': (end_lat, end_lon)
                    })

        except (json.JSONDecodeError, TypeError, IndexError):
            continue

    cur.close()
    conn.close()

    return circular_trips

if __name__ == "__main__":
    results = query10()

    # Save to JSON file
    results_file = "results/query10_final_results.json"
    with open(results_file, 'w') as f:
        json.dump({
            'query': 'Find trips whose start and end points are within 50m of each other (circular trips)',
            'max_distance_meters': 50,
            'total_circular_trips': len(results),
            'results': results
        }, f, indent=2)

    print(f"Found {len(results)} circular trips (start and end within 50m):")
    print("Trip ID | Distance (m) | Start Point | End Point")
    print("-" * 70)
    for trip in results[:20]:  # Show first 20
        print(f"{trip['trip_id']:7} | {trip['start_end_distance']:11.2f} | {trip['start_point']} | {trip['end_point']}")
    if len(results) > 20:
        print(f"... and {len(results) - 20} more trips")

    print(f"\nResults saved to {results_file}")
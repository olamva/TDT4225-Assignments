import json
import math

import mysql.connector

# Query 6: Find trips that passed within 100m of Porto City Hall
# (longitude, latitude) = (-8.62911, 41.15794)

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

def query6():
    conn = mysql.connector.connect(
        host="127.0.0.1", port=3306, user="root", password="secret", database="porto"
    )

    # Porto City Hall coordinates
    city_hall_lat = 41.15794
    city_hall_lon = -8.62911
    max_distance = 100  # 100 meters

    sql = """
    SELECT trip_id, polyline
    FROM all_taxi_info
    WHERE polyline IS NOT NULL
    """

    cur = conn.cursor()
    cur.execute(sql)

    trips_near_city_hall = []

    for trip_id, polyline_json in cur:
        try:
            polyline = json.loads(polyline_json) if isinstance(polyline_json, str) else polyline_json

            if not polyline:
                continue

            # Check each GPS point in the trip
            for point in polyline:
                if len(point) >= 2:
                    lon, lat = point[0], point[1]
                    distance = haversine_distance(lat, lon, city_hall_lat, city_hall_lon)

                    if distance <= max_distance:
                        trips_near_city_hall.append(trip_id)
                        break  # Found one point close enough, no need to check others

        except (json.JSONDecodeError, TypeError, IndexError):
            continue

    cur.close()
    conn.close()

    return trips_near_city_hall

if __name__ == "__main__":
    results = query6()

    # Save to JSON file
    results_file = "results/query6_final_results.json"
    with open(results_file, 'w') as f:
        json.dump({
            'query': 'Find trips that passed within 100m of Porto City Hall',
            'porto_city_hall_coordinates': {'latitude': 41.15794, 'longitude': -8.62911},
            'max_distance_meters': 100,
            'total_trips_found': len(results),
            'trip_ids': results
        }, f, indent=2)

    print(f"Number of trips that passed within 100m of Porto City Hall: {len(results)}")
    print("Trip IDs:")
    for trip_id in results[:10]:  # Show first 10
        print(f"  {trip_id}")
    if len(results) > 10:
        print(f"  ... and {len(results) - 10} more")

    print(f"\nResults saved to {results_file}")
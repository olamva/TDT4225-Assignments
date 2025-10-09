import json

import mysql.connector


def calculate_distance(polyline):
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

def query4b():
    conn = mysql.connector.connect(
        host="127.0.0.1", port=3306, user="root", password="secret", database="porto"
    )

    sql = """
    SELECT
        t.call_type,
        t.trip_id,
        j.polyline,
        trip_start.start_hour
    FROM trip_by_taxi t
    JOIN trip_journey j ON t.trip_id = j.trip_id
    JOIN (
        SELECT
            trip_id,
            HOUR(FROM_UNIXTIME(SUBSTRING(trip_id, 1, 10))) as start_hour
        FROM trip_journey
        GROUP BY trip_id
    ) trip_start ON t.trip_id = trip_start.trip_id
    WHERE j.polyline IS NOT NULL
    """

    cur = conn.cursor()
    cur.execute(sql)

    call_type_data = {}

    for call_type, _, polyline_json, start_hour in cur:
        if call_type not in call_type_data:
            call_type_data[call_type] = {
                'durations': [],
                'distances': [],
                'time_bands': {'00-06': 0, '06-12': 0, '12-18': 0, '18-24': 0},
                'total_trips': 0
            }

        try:
            polyline = json.loads(polyline_json) if isinstance(polyline_json, str) else polyline_json

            duration = len(polyline) * 15 if polyline else 0

            distance = calculate_distance(polyline)

            if 0 <= start_hour < 6:
                time_band = '00-06'
            elif 6 <= start_hour < 12:
                time_band = '06-12'
            elif 12 <= start_hour < 18:
                time_band = '12-18'
            else:
                time_band = '18-24'

            call_type_data[call_type]['durations'].append(duration)
            call_type_data[call_type]['distances'].append(distance)
            call_type_data[call_type]['time_bands'][time_band] += 1
            call_type_data[call_type]['total_trips'] += 1

        except (json.JSONDecodeError, TypeError):
            continue

    cur.close()
    conn.close()

    results = {}
    for call_type, data in call_type_data.items():
        total_trips = data['total_trips']
        if total_trips > 0:
            results[call_type] = {
                'avg_duration_seconds': sum(data['durations']) / len(data['durations']) if data['durations'] else 0,
                'avg_distance_meters': sum(data['distances']) / len(data['distances']) if data['distances'] else 0,
                'time_band_shares': {
                    band: count / total_trips for band, count in data['time_bands'].items()
                }
            }

    return results

if __name__ == "__main__":
    results = query4b()
    for call_type, stats in results.items():
        print(f"\nCall Type {call_type}:")
        print(f"  Average Duration: {stats['avg_duration_seconds']:.1f} seconds")
        print(f"  Average Distance: {stats['avg_distance_meters']:.1f} meters")
        print("  Time Band Shares:")
        for band, share in stats['time_band_shares'].items():
            print(f"    {band}: {share:.3f} ({share*100:.1f}%)")
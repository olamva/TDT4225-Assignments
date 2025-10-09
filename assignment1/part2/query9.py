import json
from datetime import datetime

import mysql.connector

# Query 9: Find trips that started on one calendar day and ended on the next (midnight crossers)

def query9():
    conn = mysql.connector.connect(
        host="127.0.0.1", port=3306, user="root", password="secret", database="porto"
    )

    sql = """
    SELECT
        trip_id,
        taxi_id,
        FROM_UNIXTIME(LEFT(trip_id, 10)) AS start_time,
        DATE_ADD(FROM_UNIXTIME(LEFT(trip_id, 10)), INTERVAL (JSON_LENGTH(polyline) * 15) SECOND) AS estimated_end_time,
        JSON_LENGTH(polyline) as num_gps_points
    FROM all_taxi_info
    WHERE polyline IS NOT NULL
      AND JSON_LENGTH(polyline) > 2
      AND DATE(FROM_UNIXTIME(LEFT(trip_id, 10))) !=
          DATE(DATE_ADD(FROM_UNIXTIME(LEFT(trip_id, 10)), INTERVAL (JSON_LENGTH(polyline) * 15) SECOND))
    ORDER BY FROM_UNIXTIME(LEFT(trip_id, 10))
    """

    cur = conn.cursor()
    cur.execute(sql)

    results = []
    for trip_id, taxi_id, start_time, end_time, num_points in cur:
        results.append({
            'trip_id': trip_id,
            'taxi_id': taxi_id,
            'start_time': start_time.isoformat() if start_time else None,
            'estimated_end_time': end_time.isoformat() if end_time else None,
            'num_gps_points': num_points,
            'start_date': start_time.date().isoformat() if start_time else None,
            'end_date': end_time.date().isoformat() if end_time else None
        })

    cur.close()
    conn.close()

    return results

if __name__ == "__main__":
    results = query9()

    # Save to JSON file
    results_file = "results/query9_final_results.json"
    with open(results_file, 'w') as f:
        json.dump({
            'query': 'Find trips that started on one calendar day and ended on the next (midnight crossers)',
            'total_midnight_crosser_trips': len(results),
            'results': results
        }, f, indent=2)

    print(f"Midnight crosser trips: {len(results)}")
    print("Trip ID | Taxi ID | Start Time | End Time | GPS Points")
    print("-" * 80)

    for result in results[:20]:  # Show first 20
        print(f"{result['trip_id']} | {result['taxi_id']} | {result['start_time']} | {result['estimated_end_time']} | {result['num_gps_points']}")

    if len(results) > 20:
        print(f"... and {len(results) - 20} more trips")

    print(f"\nResults saved to {results_file}")
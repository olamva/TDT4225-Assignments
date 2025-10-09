import json

import mysql.connector

# Query 4a: What is the most used call type per taxi?

def query4a():
    conn = mysql.connector.connect(
        host="127.0.0.1", port=3306, user="root", password="secret", database="porto"
    )

    sql = """
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
    ORDER BY taxi_id
    """

    cur = conn.cursor()
    cur.execute(sql)

    results = []
    for taxi_id, call_type, call_count in cur:
        results.append({
            'taxi_id': taxi_id,
            'most_used_call_type': call_type,
            'call_count': call_count
        })

    cur.close()
    conn.close()

    return results

if __name__ == "__main__":
    results = query4a()

    # Save to JSON file
    results_file = "results/query4a_final_results.json"
    with open(results_file, 'w') as f:
        json.dump({
            'query': 'What is the most used call type per taxi?',
            'total_taxis': len(results),
            'results': results
        }, f, indent=2)

    print(f"Most used call type per taxi:")
    print(f"Total taxis analyzed: {len(results)}")
    print("Taxi ID | Most Used Call Type | Count")
    print("-" * 40)

    for result in results[:20]:  # Show first 20
        print(f"{result['taxi_id']:7} | {result['most_used_call_type']:18} | {result['call_count']:5}")

    if len(results) > 20:
        print(f"... and {len(results) - 20} more taxis")

    print(f"\nResults saved to {results_file}")
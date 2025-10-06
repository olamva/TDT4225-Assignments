
import mysql.connector

conn = mysql.connector.connect(
    host="127.0.0.1",       # docker eksponerer port 3306 til localhost
    port=3306,
    user="root",
    password="secret",
    database="porto"        # databasen du opprettet i make_db.sql
)


# 1. How many taxis, trips, and total GPS points are there?

#Assume that only taxis with at least one trip registered will be in the database
def total_taxis_trips_GPS_points():
    sql = """
    SELECT
      (SELECT COUNT(DISTINCT taxi_id) FROM trip_by_taxi) AS Number_of_taxis,
      (SELECT COUNT(DISTINCT trip_id)  FROM trip_by_taxi)AS Number_of_trips,
      (SELECT COALESCE(SUM(JSON_LENGTH(polyline)), 0) FROM trip_journey
         WHERE polyline IS NOT NULL) AS Number_of_GPS_points
    """

    cur = conn.cursor()
    cur.execute(sql)
    taxis, trips, gps_points = cur.fetchone()
    cur.close()
    return {
        "Number_of_taxis": taxis,
        "Number_of_trips": trips,
        "Number_of_GPS_points": gps_points
    }


# 2. What is the average number of trips per taxi?
def average_trips_per_day():
    sql = """ 
    SELECT AVG()
    FROM trip_journey
    GROUP BY 
    WHERE
    
    """

# 3. List the top 20 taxis with the most trips.

# 4. a) What is the most used call type per taxi?

# b) For each call type, compute the average trip duration and distance, and also report the share of trips starting in four time bands: 00–06, 06–12, 12–18, and 18–24.

# 5. Find the taxis with the most total hours driven as well as total distance driven.
# List them in order of total hours.

# 6. Find the trips that passed within 100 m of Porto City Hall.
# (longitude, latitude) = (-8.62911, 41.15794)

# 7. Identify the number of invalid trips. An invalid trip is defined as a trip with fewer than 3 GPS points.

# 8. Find pairs of different taxis that were within 5m and within 5 seconds of each other at least once.

# 9. Find the trips that started on one calendar day and ended on the next (midnight crossers).

# 10.Find the trips whose start and end points are within 50 m of each other (circular trips).

# 11.For each taxi, compute the average idle time between consecutive trips. List the top 20 taxis with the highest average idle time


def answers_part2():
    print(total_taxis_trips_GPS_points())

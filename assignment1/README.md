# TDT4225 - Assignment 1

## Setup Instructions

First, create a Python virtual environment and activate it:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Download the required packages from the requirements.txt file using pip:

```bash
pip install -r requirements.txt
```

First, ensure you have the dataset downloaded locally, and move it to `data/original`. Then, create the cleaned data directory:

```bash
mkdir -p data/cleaned
```

Then, run the following script to preprocess the data:

```bash
python3 clean_dataset.py
```

> This will create a cleaned CSV file at `data/cleaned/cleaned_porto_data.csv` and a pickle file at `data/cleaned/cleaned_porto_data.pkl`.

Then set up the local MySQL server using Docker (if the container already exists, ignore this step):

```bash
docker run -d --name mysql-local \
  -e MYSQL_ROOT_PASSWORD=secret \
  -v "$(pwd):/work" \
  -p 3306:3306 \
  mysql:8.0 --local-infile=1
```

Run the make_db.sql script to load the CSV data into the database (this will take a while):

```bash
docker exec -i mysql-local \
  mysql --local-infile=1 -uroot -psecret -e "source /work/make_db.sql"
```

To connect to the MySQL server and check that the database was created, run the following in a different terminal:

```bash
docker exec -it mysql-local mysql -u root -psecret
```

Now you are free to execute SQL commands. Here's an example input to check things worked:

```sql
USE porto;
SELECT COUNT(*) FROM all_taxi_info;
```

Whenever you want to disconnect from the MySQL CLI, input the following:

```sql
EXIT;
```

### Troubleshooting

If you need to re-initialize the database, you can stop and remove the Docker container with:

```bash
docker stop mysql-local && docker rm mysql-local
```

## Part 1

To fetch the figures:

```bash
python3 visualize_porto.py
```

> The figures will be output to the `figures/` directory.

To run an EDA:

```bash
python3 eda.py
```

## Part 2

1. **How many taxis, trips, and total GPS points are there?**
2. **What is the average number of trips per taxi?**
3. **List the top 20 taxis with the most trips.**
4. **a) What is the most used call type per taxi?**
   **b) For each call type, compute the average trip duration and distance, and also
   report the share of trips starting in four time bands: 00-06, 06-12, 12-18, and
   18-24.**
5. **Find the taxis with the most total hours driven as well as total distance driven.
   List them in order of total hours.**
6. **Find the trips that passed within 100 m of Porto City Hall.
   (longitude, latitude) = (-8.62911, 41.15794)**
7. **Identify the number of invalid trips. An invalid trip is defined as a trip with fewer
   than 3 GPS points.**
8. **Find pairs of different taxis that were within 5m and within 5 seconds of each
   other at least once.**
9. **Find the trips that started on one calendar day and ended on the next (midnight
   crossers).**
10. **Find the trips whose start and end points are within 50 m of each other (circular
    trips).**
11. **For each taxi, compute the average idle time between consecutive trips. List the
    top 20 taxis with the highest average idle time.**

# TDT4225 - Assignment 1

## Setup Instructions

Recommend to setup .venv locally and activate it:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Download the required packages from the requirements.txt file using pip:

```bash
pip install -r requirements.txt
```

Then set up the local MySQL server using Docker:

```bash
docker run --name mysql-local -e MYSQL_ROOT_PASSWORD=secret -p 3306:3306 -d mysql:8.0
```

how to conect to the MySQL server later:
```bash
docker exec -it mysql-local mysql -uroot -p
```

Then run the database setup script to create the database:

```bash
python3 setup_database.py
```

Then run the example program:

```bash
python3 example.py
```

## Connecting to DB in MySQL CLI

To connect to the MySQL server using the command line interface, run:

```bash
docker exec -it mysql-local mysql -u root -psecret
```



for å sette opp db til part 2:

starte SQL med docker:
(første gang du kjører):

docker run --name mysql-local \
  -e MYSQL_ROOT_PASSWORD=secret \
  -p 3306:3306 \
  -v "$PWD":/work \
  -d mysql:8.0 --local-infile=ON

Hvis du har kjørt den før men vil starte fresh:
docker rm -f mysql-local 2>/dev/null || true
docker volume prune -f  # optional, removes all unused volumes

så kjør:

docker run --name mysql-local \
  -e MYSQL_ROOT_PASSWORD=secret \
  -p 3306:3306 \
  -v "$PWD":/work \
  -d mysql:8.0 --local-infile=ON


  koble til MySQL Server:
  docker exec -it mysql-local mysql -uroot -psecret


sett opp test database:
python3 setup_database.py

så:

python3 example.py

sett opp database for part 2:
docker exec -it mysql-local mysql -uroot -psecret -e "SET GLOBAL local_infile = 1;"


kjør så sql scriptet:

docker exec -i mysql-local \
  mysql --local-infile=1 -uroot -psecret < make_db.sql


sjekk at de importerte tabellen er der:
docker exec -it mysql-local mysql -uroot -psecret -e "SELECT COUNT(*) FROM porto.all_taxi_info;"


(valgfrit) for å fjerne alle stoppede og ubrukte volumer:
docker system prune -f
docker volume prune -f



<!--  gammel info:

for å laste make_db.sql og cleaned_porto_dat.csv inn i dokeren:

docker rm -f mysql-local 2>/dev/null || true

docker run --name mysql-local \
  -e MYSQL_ROOT_PASSWORD=secret \
  -p 3306:3306 \
  -v "$PWD":/work \
  -d mysql:8.0 --local-infile=ON


pass på at SQL peker til riktig csv fil
LOAD DATA LOCAL INFILE '/work/cleaned_porto_data.csv'


skru på LOCAL INFILE på serveren:
docker exec -it mysql-local mysql -uroot -psecret -e "SET GLOBAL local_infile=1;"

Kjør hele sql serveren på klienten:
docker exec -i mysql-local \
  mysql --local-infile=1 -uroot -psecret < make_db.sql
   -->
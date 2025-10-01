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

Then run the database setup script to create the database:

```bash
python3 setup_database.py
```

Then run the example program:

```bash
python3 example.py
```

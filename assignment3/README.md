# TDT4225 - Assignment 3

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

## MongoDB Setup

To set up MongoDB using Docker, run the following command:

```bash
docker run -d --name mongodb-local \
  -p 27017:27017 \
  -v mongodb_data:/data/db \
  -e MONGO_INITDB_ROOT_USERNAME=TEST_USER \
  -e MONGO_INITDB_ROOT_PASSWORD=test123 \
  -e MONGO_INITDB_DATABASE=DATABASE_NAME \
  mongo:latest
```

To connect to MongoDB, you can use the MongoDB Shell:

```bash
docker exec -it mongodb-local mongosh -u TEST_USER -p test123 --authenticationDatabase admin
```

Or use a Python client like pymongo in your scripts (the `DbConnector.py` class handles authentication automatically).

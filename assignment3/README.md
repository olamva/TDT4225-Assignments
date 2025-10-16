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

To set up MongoDB using Docker with data persistence, run the following command:

```bash
docker run -d --name mongodb-local \
  -p 27017:27017 \
  -v mongodb_data:/data/db \
  mongo:latest
```

This will start a MongoDB container on port 27017.

To connect to MongoDB, you can use the MongoDB Shell:

```bash
docker exec -it mongodb-local mongosh
```

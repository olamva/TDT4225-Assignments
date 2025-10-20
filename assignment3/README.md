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

### For Mac/Linux:

To set up MongoDB using Docker with authentication:

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

**Note:** Set `is_sepanta = False` in `DbConnector.py` for this setup.

### For Sepanta (Windows):

To set up MongoDB using Docker without authentication:

```bash
docker run -d --name mongodb-local \
  -p 27017:27017 \
  -v mongodb_data:/data/db \
  mongo:latest
```

To connect to MongoDB, you can use the MongoDB Shell:

```bash
docker exec -it mongodb-local mongosh
```

**Note:** Set `is_sepanta = True` in `DbConnector.py` for this setup.

---

Or use a Python client like pymongo in your scripts (the `DbConnector.py` class handles authentication automatically based on the `is_sepanta` flag).

### Troubleshooting

If you need to re-initialize the database, you can stop and remove the Docker container with:

```bash
docker stop mongodb-local && docker rm mongodb-local
```

## Queries

1. Considering only crew with job = Director, which 10 directors with ≥ 5 movies
   have the highest median revenue?
   Also report each director's movie count and mean vote_average.
2. List actor pairs have co-starred in ≥ 3 movies together, their number of
   co-appearances, and their average movie vote_average. Sort by number of
   co-appearances.
3. List the top 10 actors (with ≥ 10 credited movies) that have the widest genre
   breadth. Report the actor, the number of distinct genres they've appeared in,
   and up to 5 example genres.
4. For film collections (belongs_to_collection.name not null) with ≥ 3 movies, which
   10 collections have the largest total revenue?
   For each, report movie count, total revenue, median vote_average, and the
   earliest → latest release date.
5. By decade (e.g., 1970s, 1980s, …) and primary genre (first element in genres),
   what is the median runtime and movie count?
   List results sorted by decade then median runtime (desc).
6. For each movie's top-billed 5 cast (by order), what is the proportion of female
   cast?
   Aggregate by decade and list decades sorted by average female proportion
   (desc), including movie counts used. Unknown gender may be ignored.
7. Using a text search over overview and tagline, which 20 movies matching "noir"
   or "neo-noir" (and vote_count ≥ 50) have the highest vote_average?
   Return title, year, vote_average, and vote_count.
8. Which 20 director-actor pairs with ≥ 3 collaborations (same movie) have the
   highest mean vote_average, considering only movies with vote_count ≥ 100?
   Include the pair's films count and mean revenue.
9. Among movies where original_language ≠ "en" but at least one production
   company or country is United States, which are the top 10 original languages by
   count? For each language, report the count and one example title.
10. For each user, what is their ratings count, population variance of ratings, and
    number of distinct genres rated?
    List the top 10 most genre-diverse users, and separately the top 10
    highest-variance users (only users with ≥ 20 ratings).

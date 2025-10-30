import ast

import pandas as pd
from DbConnector import DbConnector
from pymongo import ASCENDING, DESCENDING
from tqdm import tqdm


def create_collections():
    db_connector = DbConnector(DATABASE='assignment3')
    db = db_connector.db

    print("Creating collections in 'assignment3' database...\n")

    existing_collections = db.list_collection_names()
    for collection in ['movies', 'people', 'credits', 'ratings']:
        if collection in existing_collections:
            db[collection].drop()
            print(f"Dropped existing collection: {collection}")

    print("\n" + "="*50)
    print("Creating new collections...")
    print("="*50 + "\n")

    movies_validator = {
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['id', 'title'],
            'properties': {
                'id': {
                    'bsonType': ['int', 'long'],
                    'description': 'Movie ID (primary key)'
                },
                'adult': {'bsonType': ['bool', 'string']},
                'belongs_to_collection': {'bsonType': ['object', 'string', 'null']},
                'budget': {'bsonType': ['int', 'long', 'double', 'string']},
                'genres': {'bsonType': ['array', 'string', 'null']},
                'genres_list': {'bsonType': ['array', 'string', 'null']},
                'homepage': {'bsonType': ['string', 'null']},
                'imdb_id': {'bsonType': ['string', 'null']},
                'original_language': {'bsonType': ['string', 'null']},
                'original_title': {'bsonType': ['string', 'null']},
                'overview': {'bsonType': ['string', 'null']},
                'popularity': {'bsonType': ['double', 'string']},
                'poster_path': {'bsonType': ['string', 'null']},
                'production_companies': {'bsonType': ['array', 'string', 'null']},
                'production_countries': {'bsonType': ['array', 'string', 'null']},
                'release_date': {'bsonType': ['string', 'date', 'null']},
                'revenue': {'bsonType': ['int', 'long', 'double', 'string']},
                'runtime': {'bsonType': ['double', 'string', 'null']},
                'spoken_languages': {'bsonType': ['array', 'string', 'null']},
                'status': {'bsonType': ['string', 'null']},
                'tagline': {'bsonType': ['string', 'null']},
                'title': {'bsonType': 'string'},
                'video': {'bsonType': ['bool', 'string']},
                'vote_average': {'bsonType': ['double', 'string']},
                'vote_count': {'bsonType': ['int', 'long', 'double', 'string']},
                'keywords': {'bsonType': ['array', 'string', 'null']},
                'tmdbId': {'bsonType': ['int', 'long', 'double', 'string', 'null'], 'description': 'TMDb ID from links'}
            }
        }
    }

    db.create_collection('movies', validator=movies_validator)
    db.movies.create_index([('id', ASCENDING)], unique=True)
    db.movies.create_index([('title', ASCENDING)])
    db.movies.create_index([('release_date', DESCENDING)])
    db.movies.create_index([('vote_average', DESCENDING)])
    db.movies.create_index([('tmdbId', ASCENDING)])

    people_validator = {
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['id', 'name'],
            'properties': {
                'id': {
                    'bsonType': ['int', 'long'],
                    'description': 'Person ID (primary key)'
                },
                'name': {
                    'bsonType': 'string',
                    'description': 'Person name'
                },
                'gender': {'bsonType': ['int', 'null']}
            }
        }
    }

    db.create_collection('people', validator=people_validator)
    db.people.create_index([('id', ASCENDING)], unique=True)
    db.people.create_index([('name', ASCENDING)])

    credits_validator = {
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['id'],
            'properties': {
                'id': {
                    'bsonType': ['int', 'long'],
                    'description': 'Movie ID (foreign key to movies)'
                },
                'cast': {
                    'bsonType': 'array',
                    'description': 'Array of cast members',
                    'items': {
                        'bsonType': 'object',
                        'properties': {
                            'cast_id': {'bsonType': ['int', 'null']},
                            'character': {'bsonType': ['string', 'null']},
                            'credit_id': {'bsonType': ['string', 'null']},
                            'gender': {'bsonType': ['int', 'null']},
                            'id': {'bsonType': ['int', 'null']},
                            'name': {'bsonType': ['string', 'null']},
                            'order': {'bsonType': ['int', 'null']},
                            'profile_path': {'bsonType': ['string', 'null']}
                        }
                    }
                },
                'crew': {
                    'bsonType': 'array',
                    'description': 'Array of crew members',
                    'items': {
                        'bsonType': 'object',
                        'properties': {
                            'credit_id': {'bsonType': ['string', 'null']},
                            'department': {'bsonType': ['string', 'null']},
                            'gender': {'bsonType': ['int', 'null']},
                            'id': {'bsonType': ['int', 'null']},
                            'job': {'bsonType': ['string', 'null']},
                            'name': {'bsonType': ['string', 'null']},
                            'profile_path': {'bsonType': ['string', 'null']}
                        }
                    }
                }
            }
        }
    }

    db.create_collection('credits', validator=credits_validator)
    db.credits.create_index([('id', ASCENDING)], unique=True)
    db.credits.create_index([('cast.id', ASCENDING)])
    db.credits.create_index([('crew.id', ASCENDING)])

    ratings_validator = {
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['userId', 'movieId', 'rating'],
            'properties': {
                'userId': {
                    'bsonType': ['int', 'long'],
                    'description': 'User ID'
                },
                'movieId': {
                    'bsonType': ['int', 'long'],
                    'description': 'Movie ID (foreign key to movies)'
                },
                'rating': {
                    'bsonType': 'double',
                    'description': 'Rating value (typically 0.5 to 5.0)'
                },
                'timestamp': {
                    'bsonType': ['int', 'long', 'double'],
                    'description': 'Unix timestamp'
                }
            }
        }
    }

    db.create_collection('ratings', validator=ratings_validator)
    db.ratings.create_index([('userId', ASCENDING), ('movieId', ASCENDING)])
    db.ratings.create_index([('movieId', ASCENDING)])
    db.ratings.create_index([('userId', ASCENDING)])
    db.ratings.create_index([('rating', DESCENDING)])
    db.ratings.create_index([('timestamp', DESCENDING)])

    print("\n" + "="*50)
    print("Collection creation completed!")
    print("="*50 + "\n")

    # Print collection stats
    print("Collection Statistics:")
    print("-" * 50)
    for collection_name in ['movies', 'people', 'credits', 'ratings']:
        collection = db[collection_name]
        count = collection.count_documents({})
        indexes = collection.index_information()
        print(f"\n{collection_name}:")
        print(f"  Documents: {count}")
        print(f"  Indexes: {len(indexes)}")
        for idx_name, idx_info in indexes.items():
            print(f"    - {idx_name}: {idx_info.get('key', [])}")

    return db_connector


def safe_eval(val):
    if pd.isna(val) or val == '' or val == 'None':
        return None
    if isinstance(val, (list, dict)):
        return val
    try:
        return ast.literal_eval(val)
    except (ValueError, SyntaxError):
        return val


def load_movies(db):
    print("\n" + "="*60)
    print("Loading movies...")
    print("="*60)

    df = pd.read_csv('data/movies_cleaned/movies_metadata_cleaned.csv', low_memory=False)

    print("Loading links data to merge tmdbId...")
    links_df = pd.read_csv('data/movies_cleaned/links_cleaned.csv')

    tmdb_mapping = {}
    for _, row in links_df.iterrows():
        movie_id = int(row['movieId']) if pd.notna(row['movieId']) else None
        tmdb_id = int(row['tmdbId']) if pd.notna(row['tmdbId']) and row['tmdbId'] != '' else None
        if movie_id and tmdb_id:
            tmdb_mapping[movie_id] = tmdb_id

    print(f"Loaded {len(tmdb_mapping)} tmdbId mappings from links")

    print("Loading keywords data to merge keywords (using links mapping for ID alignment)...")

    try:
        keywords_df = pd.read_csv('data/movies/keywords.csv')
        print(f"Loaded keywords file: data/movies/keywords.csv ({len(keywords_df)} rows)")
    except Exception as e:
        print(f"Could not load keywords file 'data/movies/keywords.csv': {e}")
        keywords_df = pd.DataFrame()

    tmdb_to_movieid = {v: k for k, v in tmdb_mapping.items()}

    keywords_by_tmdb = {}
    keywords_by_movieid = {}

    if not keywords_df.empty:
        for _, krow in keywords_df.iterrows():
            try:
                kid_raw = krow.get('id')
                if pd.isna(kid_raw) or kid_raw == '':
                    continue
                kid = int(kid_raw)
            except Exception:
                continue

            raw_kw = krow.get('keywords') if 'keywords' in keywords_df.columns else None
            raw_kw_parsed = safe_eval(raw_kw) if pd.notna(raw_kw) else None

            names = []
            if isinstance(raw_kw_parsed, list):
                for it in raw_kw_parsed:
                    if isinstance(it, dict):
                        name = it.get('name')
                        if name:
                            names.append(name)
                    elif isinstance(it, str):
                        names.append(it)
            elif isinstance(raw_kw_parsed, dict):
                name = raw_kw_parsed.get('name') if raw_kw_parsed else None
                if name:
                    names.append(name)
            elif isinstance(raw_kw_parsed, str):
                names.append(raw_kw_parsed)

            if kid in tmdb_to_movieid:
                keywords_by_tmdb[kid] = names
                keywords_by_movieid[tmdb_to_movieid[kid]] = names
            elif kid in tmdb_mapping:
                tmdb = tmdb_mapping.get(kid)
                if tmdb:
                    keywords_by_movieid[kid] = names
                    keywords_by_tmdb[tmdb] = names
            else:
                keywords_by_tmdb[kid] = names

    print(f"Prepared keywords_by_tmdb entries: {len(keywords_by_tmdb)}; keywords_by_movieid entries: {len(keywords_by_movieid)}")

    initial_count = len(df)
    duplicate_ids = df[df.duplicated(subset=['id'], keep='first')]['id'].tolist()

    if duplicate_ids:
        print(f"Found {len(duplicate_ids)} duplicate movie IDs:")
        for dup_id in sorted(set(duplicate_ids)):
            dup_rows = df[df['id'] == dup_id].reset_index(drop=True)
            print(f"\n  ID {dup_id}: appears {len(dup_rows)} times")
            for i in range(len(dup_rows)):
                print(f"      Occurrence {i+1}: {dup_rows.iloc[i]['title']}")

            if len(dup_rows) > 1:
                print(f"      Differences:")
                first_row = dup_rows.iloc[0]
                for i in range(1, len(dup_rows)):
                    current_row = dup_rows.iloc[i]
                    differences = []
                    for col in dup_rows.columns:
                        val1 = first_row[col]
                        val2 = current_row[col]
                        if pd.isna(val1) and pd.isna(val2):
                            continue
                        elif pd.isna(val1) or pd.isna(val2) or str(val1) != str(val2):
                            differences.append(f"{col}")

                    if differences:
                        print(f"        Occurrence 1 vs {i+1}: {', '.join(differences)}")
                        for field in differences:
                            print(f"          - {field}: '{first_row[field]}' vs '{current_row[field]}'")
                    else:
                        print(f"        Occurrence 1 vs {i+1}: Identical")

    df = df.drop_duplicates(subset=['id'], keep='first')
    duplicates_removed = initial_count - len(df)
    if duplicates_removed > 0:
        print(f"\n Removed {duplicates_removed} duplicate entries (keeping first occurrence)\n")

    movies = []
    seen_ids = set()

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Processing movies"):
        movie_id = int(row['id']) if pd.notna(row['id']) else None

        if movie_id is None or movie_id in seen_ids:
            continue

        seen_ids.add(movie_id)

        movie = {
            'id': movie_id,
            'belongs_to_collection': safe_eval(row['belongs_to_collection']),
            'budget': int(row['budget']) if pd.notna(row['budget']) and row['budget'] != '' else 0,
            'genres': safe_eval(row['genres']),
            'genres_list': safe_eval(row['genres_list']),
            'imdb_id': row['imdb_id'] if pd.notna(row['imdb_id']) else None,
            'original_language': row['original_language'] if pd.notna(row['original_language']) else None,
            'production_companies': safe_eval(row['production_companies']),
            'production_countries': safe_eval(row['production_countries']),
            'release_date': row['release_date'] if pd.notna(row['release_date']) else None,
            'revenue': int(row['revenue']) if pd.notna(row['revenue']) and row['revenue'] != '' else 0,
            'runtime': float(row['runtime']) if pd.notna(row['runtime']) and row['runtime'] != '' else None,
            'spoken_languages': safe_eval(row['spoken_languages']),
            'title': row['title'] if pd.notna(row['title']) else '',
            'vote_average': float(row['vote_average']) if pd.notna(row['vote_average']) else 0.0,
            'vote_count': int(row['vote_count']) if pd.notna(row['vote_count']) else 0,
            # Prefer keywords_by_tmdb lookup since movies metadata 'id' is TMDb ID
            'keywords': keywords_by_tmdb.get(movie_id, []),
        }

        movies.append(movie)

    if movies:
        result = db.movies.insert_many(movies, ordered=False)
        print(f" Inserted {len(result.inserted_ids)} movies")
    else:
        print("No movies to insert")


def load_credits(db):
    print("\n" + "="*60)
    print("Loading credits...")
    print("="*60)

    df = pd.read_csv('data/movies_cleaned/credits_cleaned.csv')

    initial_count = len(df)
    duplicate_ids = df[df.duplicated(subset=['id'], keep='first')]['id'].tolist()

    if duplicate_ids:
        print(f"Found {len(duplicate_ids)} duplicate credit IDs:")
        for dup_id in sorted(set(duplicate_ids)):
            dup_rows = df[df['id'] == dup_id].reset_index(drop=True)
            print(f"\n ID {dup_id}: appears {len(dup_rows)} times")

            if len(dup_rows) > 1:
                first_row = dup_rows.iloc[0]
                for i in range(1, len(dup_rows)):
                    current_row = dup_rows.iloc[i]

                    cast1 = str(first_row['cast'])
                    cast2 = str(current_row['cast'])
                    crew1 = str(first_row['crew'])
                    crew2 = str(current_row['crew'])

                    differences = []
                    if cast1 != cast2:
                        differences.append('cast')
                    if crew1 != crew2:
                        differences.append('crew')

                    if differences:
                        print(f"      Occurrence 1 vs {i+1}: Different fields: {', '.join(differences)}")
                    else:
                        print(f"      Occurrence 1 vs {i+1}: Identical")

    df = df.drop_duplicates(subset=['id'], keep='first')
    duplicates_removed = initial_count - len(df)
    if duplicates_removed > 0:
        print(f"\n Removed {duplicates_removed} duplicate entries (keeping first occurrence)\n")

    credits = []
    seen_ids = set()

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Processing credits"):
        credit_id = int(row['id']) if pd.notna(row['id']) else None

        if credit_id is None or credit_id in seen_ids:
            continue

        seen_ids.add(credit_id)

        credit = {
            'id': credit_id,
            'cast': safe_eval(row['cast']) if pd.notna(row['cast']) else [],
            'crew': safe_eval(row['crew']) if pd.notna(row['crew']) else []
        }

        credits.append(credit)

    if credits:
        result = db.credits.insert_many(credits, ordered=False)
        print(f" Inserted {len(result.inserted_ids)} credit records")
    else:
        print(" No credits to insert")


def load_people(db):
    print("\n" + "="*60)
    print("Extracting people from credits...")
    print("="*60)

    credits = list(db.credits.find())

    people_dict = {}

    for credit in tqdm(credits, desc="Processing credits for people"):
        if credit.get('cast'):
            for person in credit['cast']:
                person_id = person.get('id')
                if person_id and person_id not in people_dict:
                    people_dict[person_id] = {
                        'id': person_id,
                        'name': person.get('name'),
                        'gender': person.get('gender')
                    }

        if credit.get('crew'):
            for person in credit['crew']:
                person_id = person.get('id')
                if person_id and person_id not in people_dict:
                    people_dict[person_id] = {
                        'id': person_id,
                        'name': person.get('name'),
                        'gender': person.get('gender')
                    }

    people = list(people_dict.values())

    if people:
        result = db.people.insert_many(people)
        print(f" Inserted {len(result.inserted_ids)} unique people")
    else:
        print(" No people to insert")


def load_ratings(db, sample_size=None):
    print("\n" + "="*60)
    print("Loading ratings...")
    if sample_size:
        print(f"(Sampling {sample_size} records for faster testing)")
    print("="*60)

    df = pd.read_csv('data/movies_cleaned/ratings_cleaned.csv')

    if sample_size:
        df = df.sample(n=min(sample_size, len(df)))

    ratings = []
    batch_size = 10000

    for i in tqdm(range(0, len(df), batch_size), desc="Processing ratings in batches"):
        batch = df.iloc[i:i+batch_size]

        for _, row in batch.iterrows():
            rating = {
                'userId': int(row['userId']) if pd.notna(row['userId']) else None,
                'movieId': int(row['movieId']) if pd.notna(row['movieId']) else None,
                'rating': float(row['rating']) if pd.notna(row['rating']) else None,
                'timestamp': int(row['timestamp']) if pd.notna(row['timestamp']) else None
            }

            if all([rating['userId'], rating['movieId'], rating['rating']]):
                ratings.append(rating)

        if ratings:
            db.ratings.insert_many(ratings)
            ratings = []

    if ratings:
        db.ratings.insert_many(ratings)

    total_count = db.ratings.count_documents({})
    print(f" Inserted {total_count} rating records")


def load_all_data(db_connector):
    print("\n" + "="*60)
    print("LOADING DATA INTO MONGODB")
    print("="*60)

    db = db_connector.db

    try:
        load_movies(db)
        load_credits(db)
        load_people(db)

        print("\n" + "="*60)
        print("Ratings file can be very large!")
        print("Options:")
        print("  1. Load all ratings (may take several minutes)")
        print("  2. Load sample (10000 ratings)")
        print("  3. Skip ratings")
        choice = input("Enter choice (1/2/3): ").strip()

        if choice == '1':
            load_ratings(db)
        elif choice == '2':
            load_ratings(db, sample_size=10000)
        else:
            print("Skipping ratings...")

        print("\n" + "="*60)
        print("FINAL STATISTICS")
        print("="*60)
        print(f"Movies:   {db.movies.count_documents({})}")
        print(f"Credits:  {db.credits.count_documents({})}")
        print(f"People:   {db.people.count_documents({})}")
        print(f"Ratings:  {db.ratings.count_documents({})}")
        print("="*60)

        print("\n Data loading complete!")
        return True

    except Exception as e:
        print(f"\n Error loading data: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    print("\n" + "="*60)
    print("MONGODB SETUP FOR ASSIGNMENT 3")
    print("="*60)

    success = True
    db_connector = None

    try:
        db_connector = create_collections()

        print("\n" + "="*60)
        print("Do you want to load data from CSV files?")
        load_data = input("Load data? (y/n): ").strip().lower()

        if load_data == 'y':
            success = load_all_data(db_connector)
        else:
            print("\nSkipping data loading. Run this script again to load data later.")

    except Exception as e:
        print(f"\n Setup failed: {e}")
        import traceback
        traceback.print_exc()
        success = False

    finally:
        if db_connector:
            db_connector.close_connection()

        if success:
            print("\n Setup complete!")
        else:
            print("\n Setup failed. Please check the errors above.")
            print("\n Setup failed. Please check the errors above.")

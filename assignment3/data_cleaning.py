import ast
from pathlib import Path

import numpy as np
import pandas as pd


def clean_movies_runtime(df):
    original_rows = len(df)
    print(f"Original movies_metadata rows: {original_rows}")

    df = df[df['status'] == 'Released'].copy()
    rows_removed = original_rows - len(df)
    print(f"Rows removed (non-released movies): {rows_removed}")
    print(f"Rows after cleaning: {len(df)}")

    cleaned_df = df.copy()
    zero_runtime_mask = cleaned_df['runtime'] == 0

    if zero_runtime_mask.sum() > 0:
        print(f"Cleaning {zero_runtime_mask.sum()} movies with 0 runtime...")

        if 'genres_list' not in cleaned_df.columns:
            def extract_genres(genre_str):
                try:
                    genres = ast.literal_eval(genre_str)
                    return [g['name'] for g in genres] if isinstance(genres, list) else []
                except:
                    return []
            cleaned_df['genres_list'] = cleaned_df['genres'].apply(extract_genres)

        genre_medians = {}
        for genre_list in cleaned_df['genres_list']:
            if genre_list:
                for genre in genre_list:
                    if genre not in genre_medians:
                        genre_movies = cleaned_df[
                            (cleaned_df['genres_list'].apply(lambda x: genre in x if x else False)) &
                            (cleaned_df['runtime'] > 0)
                        ]
                        if not genre_movies.empty:
                            genre_medians[genre] = genre_movies['runtime'].median()

        for idx in cleaned_df[zero_runtime_mask].index:
            movie_genres = cleaned_df.loc[idx, 'genres_list']
            if movie_genres:
                genre_runtimes = [genre_medians.get(genre, np.nan) for genre in movie_genres]
                valid_runtimes = [rt for rt in genre_runtimes if not np.isnan(rt)]
                if valid_runtimes:
                    median_runtime = np.median(valid_runtimes)
                    cleaned_df.loc[idx, 'runtime'] = median_runtime

    return cleaned_df


def fix_vote_counts(df, ratings_df, links_df):
    print("\n" + "="*70)
    print("Calculating actual vote counts and averages from ratings...")
    print("="*70)

    # Calculate both count and average rating per movieId
    ratings_stats = ratings_df.groupby('movieId')['rating'].agg(['count', 'mean']).to_dict()
    ratings_counts = ratings_stats['count']
    ratings_averages = ratings_stats['mean']
    print(f"Calculated vote counts and averages for {len(ratings_counts)} unique movieIds")

    links_df_converted = links_df.copy()
    links_df_converted['tmdbId'] = pd.to_numeric(links_df_converted['tmdbId'], errors='coerce')
    links_df_converted['movieId'] = pd.to_numeric(links_df_converted['movieId'], errors='coerce')
    links_df_converted = links_df_converted.dropna(subset=['tmdbId', 'movieId'])
    links_df_converted['tmdbId'] = links_df_converted['tmdbId'].astype(int)
    links_df_converted['movieId'] = links_df_converted['movieId'].astype(int)
    print(f"Mapped vote counts for {len(links_df_converted)} unique tmdbIds")

    vote_counts = {}
    vote_averages = {}
    for _, link_row in links_df_converted.iterrows():
        tmdb_id = link_row['tmdbId']
        movie_id = link_row['movieId']

        if movie_id in ratings_counts:
            if tmdb_id in vote_counts:
                # For duplicates, we need to weighted average
                old_count = vote_counts[tmdb_id]
                old_avg = vote_averages[tmdb_id]
                new_count = ratings_counts[movie_id]
                new_avg = ratings_averages[movie_id]
                
                total_count = old_count + new_count
                weighted_avg = (old_avg * old_count + new_avg * new_count) / total_count
                
                vote_counts[tmdb_id] = total_count
                vote_averages[tmdb_id] = weighted_avg
            else:
                vote_counts[tmdb_id] = ratings_counts[movie_id]
                vote_averages[tmdb_id] = ratings_averages[movie_id]

    print(f"Mapped vote counts and averages for {len(vote_counts)} movies (tmdbId -> movieId -> ratings)")

    df['id_int'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64')

    correct_count = 0
    incorrect_count = 0
    missing_count = 0
    not_in_ratings = 0

    df['vote_count'] = pd.to_numeric(df['vote_count'], errors='coerce')
    df['vote_average'] = pd.to_numeric(df['vote_average'], errors='coerce')

    for idx, row in df.iterrows():
        tmdb_id = row['id_int']

        if pd.isna(tmdb_id):
            missing_count += 1
            continue

        if tmdb_id in vote_counts:
            actual_count = vote_counts[tmdb_id]
            actual_average = vote_averages[tmdb_id]
            csv_count = row['vote_count']

            if pd.notna(csv_count) and int(csv_count) == actual_count:
                correct_count += 1
            else:
                incorrect_count += 1
                df.at[idx, 'vote_count'] = actual_count
                df.at[idx, 'vote_average'] = actual_average
        else:
            not_in_ratings += 1
            if pd.isna(row['vote_count']):
                df.at[idx, 'vote_count'] = 0

    df = df.drop(columns=['id_int'])

    print("\n" + "="*70)
    print("SUMMARY STATISTICS - BEFORE FIXING")
    print("="*70)
    print(f"Total movies in metadata: {len(df)}")
    print(f"Movies found in ratings: {len(df) - not_in_ratings}")
    print(f"Movies NOT in ratings: {not_in_ratings}")
    print()
    print(f"Correct vote_count:     {correct_count} ({correct_count/len(df)*100:.1f}%)")
    print(f"Incorrect vote_count:   {incorrect_count} ({incorrect_count/len(df)*100:.1f}%) - FIXED")
    print(f"Missing vote_count:     {missing_count} ({missing_count/len(df)*100:.1f}%)")
    print("="*70)

    print("\n" + "="*70)
    print("VERIFYING FIXED VOTE COUNTS AND AVERAGES...")
    print("="*70)

    df['id_int'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64')

    verified_correct_count = 0
    verified_incorrect_count = 0
    verified_correct_average = 0
    verified_incorrect_average = 0
    verified_missing = 0
    verified_not_in_ratings = 0

    for idx, row in df.iterrows():
        tmdb_id = row['id_int']
        csv_count = row['vote_count']
        csv_average = row['vote_average']

        if pd.isna(tmdb_id):
            verified_missing += 1
            continue

        if tmdb_id in vote_counts:
            actual_count = vote_counts[tmdb_id]
            actual_average = vote_averages[tmdb_id]
            
            # Check vote count
            if pd.notna(csv_count) and int(csv_count) == actual_count:
                verified_correct_count += 1
            else:
                verified_incorrect_count += 1
                
            # Check vote average (with tolerance for floating point comparison)
            if pd.notna(csv_average) and abs(csv_average - actual_average) < 0.01:
                verified_correct_average += 1
            else:
                verified_incorrect_average += 1
        else:
            verified_not_in_ratings += 1

    df = df.drop(columns=['id_int'])

    print("\n" + "="*70)
    print("SUMMARY STATISTICS - AFTER FIXING")
    print("="*70)
    print(f"Total movies in metadata: {len(df)}")
    print(f"Movies found in ratings: {len(df) - verified_not_in_ratings}")
    print(f"Movies NOT in ratings: {verified_not_in_ratings}")
    print()
    print(f"[OK] Correct vote_count:     {verified_correct_count} ({verified_correct_count/len(df)*100:.1f}%)")
    print(f"[X]  Incorrect vote_count:   {verified_incorrect_count} ({verified_incorrect_count/len(df)*100:.1f}%)")
    print(f"[OK] Correct vote_average:   {verified_correct_average} ({verified_correct_average/len(df)*100:.1f}%)")
    print(f"[X]  Incorrect vote_average: {verified_incorrect_average} ({verified_incorrect_average/len(df)*100:.1f}%)")
    print(f"[!]  Missing vote_count:     {verified_missing} ({verified_missing/len(df)*100:.1f}%)")
    print("="*70)

    if verified_incorrect_count == 0 and verified_incorrect_average == 0 and verified_missing == 0:
        print("SUCCESS! All vote counts and averages have been fixed correctly!")
    else:
        issues = verified_incorrect_count + verified_incorrect_average + verified_missing
        print(f"WARNING: {issues} vote counts/averages still need attention")
    print("="*70)

    return df


def merge_duplicate_movies(df, ratings_df=None, links_df=None):
    print(f"\nChecking for duplicate movie IDs...")
    initial_count = len(df)

    df['popularity'] = pd.to_numeric(df['popularity'], errors='coerce')

    vote_counts = {}
    links_df_converted = None

    if ratings_df is not None and links_df is not None:
        print("Calculating vote counts from ratings data...")
        ratings_counts = ratings_df.groupby('movieId').size().to_dict()
        print(f"Calculated {len(ratings_counts)} vote counts from ratings")

        print("Converting links DataFrame types...")
        links_df_converted = links_df.copy()
        links_df_converted['tmdbId'] = pd.to_numeric(links_df_converted['tmdbId'], errors='coerce')
        links_df_converted['movieId'] = pd.to_numeric(links_df_converted['movieId'], errors='coerce')
        links_df_converted = links_df_converted.dropna(subset=['tmdbId', 'movieId'])
        links_df_converted['tmdbId'] = links_df_converted['tmdbId'].astype(int)
        links_df_converted['movieId'] = links_df_converted['movieId'].astype(int)

        test_match = links_df_converted[links_df_converted['tmdbId'] == 105045]
        print(f"DEBUG: tmdbId 105045 found in links? {len(test_match)} matches")
        if len(test_match) > 0:
            print(f"  Sample rows: {test_match[['movieId', 'tmdbId']].head()}")

        print("Mapping tmdbId to movieId using links...")
        for _, link_row in links_df_converted.iterrows():
            tmdb_id = link_row['tmdbId']
            movie_id = link_row['movieId']

            if movie_id in ratings_counts:
                if tmdb_id in vote_counts:
                    vote_counts[tmdb_id] += ratings_counts[movie_id]
                else:
                    vote_counts[tmdb_id] = ratings_counts[movie_id]

        print(f"Mapped vote counts for {len(vote_counts)} movies (tmdbId -> movieId -> ratings)")

    duplicates = df[df.duplicated(subset=['id'], keep=False)]

    if len(duplicates) == 0:
        print("No duplicates found!")
        return df

    duplicate_ids = duplicates['id'].unique()
    print(f"Found {len(duplicate_ids)} unique movie IDs with duplicates")

    merged_rows = []

    for movie_id_str in duplicate_ids:
        movie_id = int(float(movie_id_str))
        dup_rows = df[df['id'] == movie_id_str].copy()

        if len(dup_rows) > 1:
            merged = dup_rows.iloc[0].copy()

            if movie_id == 105045:
                print(f"  DEBUG 105045: Processing duplicate, {len(dup_rows)} rows")
                print(f"  DEBUG 105045: vote_count in vote_counts? {movie_id in vote_counts}")

            popularity_values = dup_rows['popularity'].dropna()
            if len(popularity_values) > 1:
                unique_popularity = popularity_values.unique()
                if len(unique_popularity) > 1:
                    merged['popularity'] = popularity_values.mean()
                    print(f"  ID {movie_id}: Averaged popularity from {list(popularity_values)} to {merged['popularity']:.6f}")

            vote_count_values = dup_rows['vote_count'].dropna()
            if len(vote_count_values) > 1:
                unique_vote_counts = vote_count_values.unique()
                if len(unique_vote_counts) > 1:
                    if movie_id in vote_counts:
                        old_vote_counts = list(vote_count_values)
                        merged['vote_count'] = vote_counts[movie_id]
                        print(f"  ID {movie_id}: Set vote_count to {merged['vote_count']} from ratings (CSV had conflicting {old_vote_counts})")
                    else:
                        merged['vote_count'] = int(vote_count_values.mean())
                        print(f"  ID {movie_id}: Averaged vote_count from {list(vote_count_values)} to {merged['vote_count']} (no ratings data)")

            merged_rows.append(merged)

    df_no_dupes = df[~df['id'].isin(duplicate_ids)].copy()
    merged_df = pd.concat([df_no_dupes, pd.DataFrame(merged_rows)], ignore_index=True)

    print(f"\n[OK] Merged {initial_count - len(merged_df)} duplicate entries")
    print(f"Final movie count: {len(merged_df)}")

    return merged_df


def merge_duplicate_credits(df):
    print(f"\nChecking for duplicate credit IDs...")
    initial_count = len(df)

    duplicates = df[df.duplicated(subset=['id'], keep=False)]

    if len(duplicates) == 0:
        print("No duplicates found!")
        return df

    duplicate_ids = duplicates['id'].unique()
    print(f"Found {len(duplicate_ids)} unique credit IDs with duplicates")

    merged_rows = []

    for credit_id in duplicate_ids:
        dup_rows = df[df['id'] == credit_id].copy()

        if len(dup_rows) > 1:
            all_cast = []
            all_crew = []

            for _, row in dup_rows.iterrows():
                cast = ast.literal_eval(row['cast']) if pd.notna(row['cast']) and row['cast'] != '' else []
                crew = ast.literal_eval(row['crew']) if pd.notna(row['crew']) and row['crew'] != '' else []
                all_cast.extend(cast)
                all_crew.extend(crew)

            crew_lists = []
            for _, row in dup_rows.iterrows():
                crew = ast.literal_eval(row['crew']) if pd.notna(row['crew']) and row['crew'] != '' else []
                crew_lists.append(crew)

            if len(set(str(sorted(c, key=lambda x: x.get('credit_id', ''))) for c in crew_lists)) > 1:
                print(f"\n  ID {credit_id}: Crew differs between occurrences")

                for i, crew_list in enumerate(crew_lists, 1):
                    print(f"    Occurrence {i}: {len(crew_list)} crew members")

                if len(crew_lists) == 2:
                    crew1_ids = {c.get('credit_id'): c for c in crew_lists[0] if c.get('credit_id')}
                    crew2_ids = {c.get('credit_id'): c for c in crew_lists[1] if c.get('credit_id')}

                    only_in_1 = set(crew1_ids.keys()) - set(crew2_ids.keys())
                    only_in_2 = set(crew2_ids.keys()) - set(crew1_ids.keys())

                    if only_in_1:
                        print(f"    Only in occurrence 1 ({len(only_in_1)} members):")
                        for credit_id_key in list(only_in_1)[:5]:
                            person = crew1_ids[credit_id_key]
                            print(f"      - {person.get('name', 'Unknown')} as {person.get('job', 'Unknown')}")
                        if len(only_in_1) > 5:
                            print(f"      ... and {len(only_in_1) - 5} more")

                    if only_in_2:
                        print(f"    Only in occurrence 2 ({len(only_in_2)} members):")
                        for credit_id_key in list(only_in_2)[:5]:
                            person = crew2_ids[credit_id_key]
                            print(f"      - {person.get('name', 'Unknown')} as {person.get('job', 'Unknown')}")
                        if len(only_in_2) > 5:
                            print(f"      ... and {len(only_in_2) - 5} more")

            seen_cast_ids = set()
            unique_cast = []
            for person in all_cast:
                credit_id_key = person.get('credit_id')
                if credit_id_key and credit_id_key not in seen_cast_ids:
                    seen_cast_ids.add(credit_id_key)
                    unique_cast.append(person)

            seen_crew_ids = set()
            unique_crew = []
            for person in all_crew:
                credit_id_key = person.get('credit_id')
                if credit_id_key and credit_id_key not in seen_crew_ids:
                    seen_crew_ids.add(credit_id_key)
                    unique_crew.append(person)

            merged = dup_rows.iloc[0].copy()
            merged['cast'] = str(unique_cast)
            merged['crew'] = str(unique_crew)

            if len(unique_crew) > len(ast.literal_eval(dup_rows.iloc[0]['crew']) if pd.notna(dup_rows.iloc[0]['crew']) and dup_rows.iloc[0]['crew'] != '' else []):
                print(f"  ID {credit_id}: Merged crew from {[len(ast.literal_eval(row['crew']) if pd.notna(row['crew']) and row['crew'] != '' else []) for _, row in dup_rows.iterrows()]} to {len(unique_crew)} unique members")

            merged_rows.append(merged)

    df_no_dupes = df[~df['id'].isin(duplicate_ids)].copy()
    merged_df = pd.concat([df_no_dupes, pd.DataFrame(merged_rows)], ignore_index=True)

    print(f"\n[OK] Merged {initial_count - len(merged_df)} duplicate entries")
    print(f"Final credits count: {len(merged_df)}")

    return merged_df

def clean_credits_crew(df):
    original_rows = len(df)
    print(f"Original credits rows: {original_rows}")

    df['crew_parsed'] = df['crew'].apply(lambda x: ast.literal_eval(x) if pd.notnull(x) and x != '' else [])

    valid_crew_mask = ~((df['crew'].isnull() | (df['crew'] == '') | (df['crew_parsed'].apply(lambda x: len(x) == 0))))

    cleaned_df = df[valid_crew_mask].copy()
    cleaned_df = cleaned_df.drop(columns=['crew_parsed'])

    rows_removed = original_rows - len(cleaned_df)
    print(f"Rows removed: {rows_removed}")
    print(f"Rows after cleaning: {len(cleaned_df)}")

    return cleaned_df

def clean_keywords(df):
    original_rows = len(df)
    print(f"Original keywords rows: {original_rows}")

    df['keywords_parsed'] = df['keywords'].apply(lambda x: ast.literal_eval(x) if pd.notnull(x) and x != '' else [])

    valid_keywords_mask = ~((df['keywords'].isnull() | (df['keywords'] == '') | (df['keywords_parsed'].apply(lambda x: len(x) == 0))))

    cleaned_df = df[valid_keywords_mask].copy()
    cleaned_df = cleaned_df.drop(columns=['keywords_parsed'])

    rows_removed = original_rows - len(cleaned_df)
    print(f"Rows removed: {rows_removed}")
    print(f"Rows after cleaning: {len(cleaned_df)}")

    print(f"After removing empty keywords: {len(cleaned_df)} rows (removed {len(df) - len(cleaned_df)} rows)")

    initial_count = len(cleaned_df)
    duplicate_mask = cleaned_df.duplicated(subset=['id'], keep='first')
    if duplicate_mask.sum() > 0:
        print(f"Found {duplicate_mask.sum()} duplicate keyword entries")
        cleaned_df = cleaned_df[~duplicate_mask]
        print(f"Removed {initial_count - len(cleaned_df)} duplicate entries")

    print(f"Final keywords rows: {len(cleaned_df)}")
    return cleaned_df


def clean_links(df):
    print(f"Original links rows: {len(df)}")

    initial_count = len(df)
    duplicate_mask = df.duplicated(subset=['movieId'], keep='first')
    if duplicate_mask.sum() > 0:
        print(f"Found {duplicate_mask.sum()} duplicate link entries")
        cleaned_df = df[~duplicate_mask].copy()
        print(f"Removed {initial_count - len(cleaned_df)} duplicate entries")
    else:
        cleaned_df = df.copy()
        print("No duplicates found")

    print(f"Final links rows: {len(cleaned_df)}")
    return cleaned_df


def clean_ratings(df):
    print(f"Original ratings rows: {len(df)}")

    initial_count = len(df)
    df_sorted = df.sort_values('timestamp', ascending=False)

    duplicate_mask = df_sorted.duplicated(subset=['userId', 'movieId'], keep='first')
    if duplicate_mask.sum() > 0:
        print(f"Found {duplicate_mask.sum()} duplicate rating entries")
        cleaned_df = df_sorted[~duplicate_mask].copy()
        print(f"Removed {initial_count - len(cleaned_df)} duplicate entries (kept most recent)")
    else:
        cleaned_df = df_sorted.copy()
        print("No duplicates found")

    print(f"Final ratings rows: {len(cleaned_df)}")
    return cleaned_df

def save_cleaned_credits(df, output_path='data/movies_cleaned/credits_cleaned.csv'):
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False)
    print(f"Cleaned credits saved to {output_file}")

def save_cleaned_keywords(df, output_path='data/movies_cleaned/keywords_cleaned.csv'):
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False)
    print(f"Cleaned keywords saved to {output_file}")

def save_cleaned_movies(df, output_path='data/movies_cleaned/movies_metadata_cleaned.csv'):
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False)
    print(f"Cleaned data saved to {output_file}")

if __name__ == '__main__':
    data_files = {
        'movies_metadata.csv': 'data/movies_cleaned/movies_metadata_cleaned.csv',
        'credits.csv': 'data/movies_cleaned/credits_cleaned.csv',
        'keywords.csv': 'data/movies_cleaned/keywords_cleaned.csv',
        'links.csv': 'data/movies_cleaned/links_cleaned.csv',
        'links_small.csv': 'data/movies_cleaned/links_small_cleaned.csv',
        'ratings.csv': 'data/movies_cleaned/ratings_cleaned.csv',
        'ratings_small.csv': 'data/movies_cleaned/ratings_small_cleaned.csv'
    }

    ratings_df = None
    links_df = None

    ratings_path = Path('data/movies') / 'ratings.csv'
    if ratings_path.exists():
        print("Loading ratings data for vote count calculation...")
        ratings_df = pd.read_csv(ratings_path, low_memory=False)
        print(f"Loaded {len(ratings_df)} ratings")

    links_path = Path('data/movies') / 'links.csv'
    if links_path.exists():
        print("Loading links data to map movieId to tmdbId...")
        links_df = pd.read_csv(links_path, low_memory=False)
        print(f"Loaded {len(links_df)} links")

    for input_file, output_file in data_files.items():
        data_path = Path('data/movies') / input_file
        if data_path.exists():
            print(f"\n{'='*60}")
            print(f"Processing {input_file}...")
            print('='*60)
            df = pd.read_csv(data_path, low_memory=False)
            original_rows = len(df)

            if input_file == 'movies_metadata.csv':
                cleaned_df = clean_movies_runtime(df)
                if ratings_df is not None and links_df is not None:
                    cleaned_df = fix_vote_counts(cleaned_df, ratings_df, links_df)
                else:
                    print("WARNING: ratings or links data not available, skipping vote count correction")
                cleaned_df = merge_duplicate_movies(cleaned_df, ratings_df, links_df)
                save_cleaned_movies(cleaned_df, output_file)
            elif input_file == 'credits.csv':
                cleaned_df = clean_credits_crew(df)
                cleaned_df = merge_duplicate_credits(cleaned_df)
                save_cleaned_credits(cleaned_df, output_file)
            elif input_file == 'keywords.csv':
                cleaned_df = clean_keywords(df)
                save_cleaned_keywords(cleaned_df, output_file)
            elif input_file in ['links.csv', 'links_small.csv']:
                cleaned_df = clean_links(df)
                output_path = Path(output_file)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                cleaned_df.to_csv(output_path, index=False)
                print(f"Cleaned data saved to {output_path}")
            elif input_file in ['ratings.csv', 'ratings_small.csv']:
                cleaned_df = clean_ratings(df)
                output_path = Path(output_file)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                cleaned_df.to_csv(output_path, index=False)
                print(f"Cleaned data saved to {output_path}")
            else:
                print(f"Original {input_file} rows: {original_rows}")
                print(f"Rows removed: 0")
                print(f"Rows after cleaning: {original_rows}")
                output_path = Path(output_file)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                df.to_csv(output_path, index=False)
                print(f"Data saved to {output_path} (no cleaning applied)")
        else:
            print(f"Data file not found: {data_path}")
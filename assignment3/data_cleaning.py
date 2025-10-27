import ast
from pathlib import Path

import numpy as np
import pandas as pd


def clean_movies_runtime(df):
    """
    Clean movies with 0 runtime by assigning median runtime of their genre(s).
    Also filter to only include released movies.

    Args:
        df: DataFrame with movies_metadata

    Returns:
        cleaned_df: DataFrame with cleaned runtime data and only released movies
    """
    original_rows = len(df)
    print(f"Original movies_metadata rows: {original_rows}")

    # First filter to only released movies
    df = df[df['status'] == 'Released'].copy()
    rows_removed = original_rows - len(df)
    print(f"Rows removed (non-released movies): {rows_removed}")
    print(f"Rows after cleaning: {len(df)}")

    cleaned_df = df.copy()
    zero_runtime_mask = cleaned_df['runtime'] == 0

    if zero_runtime_mask.sum() > 0:
        print(f"Cleaning {zero_runtime_mask.sum()} movies with 0 runtime...")

        # Parse genres if not already done
        if 'genres_list' not in cleaned_df.columns:
            def extract_genres(genre_str):
                try:
                    genres = ast.literal_eval(genre_str)
                    return [g['name'] for g in genres] if isinstance(genres, list) else []
                except:
                    return []
            cleaned_df['genres_list'] = cleaned_df['genres'].apply(extract_genres)

        # Calculate median runtime by genre
        genre_medians = {}
        for genre_list in cleaned_df['genres_list']:
            if genre_list:
                for genre in genre_list:
                    if genre not in genre_medians:
                        # Find median runtime for this genre (excluding 0 runtime movies)
                        genre_movies = cleaned_df[
                            (cleaned_df['genres_list'].apply(lambda x: genre in x if x else False)) &
                            (cleaned_df['runtime'] > 0)
                        ]
                        if not genre_movies.empty:
                            genre_medians[genre] = genre_movies['runtime'].median()

        # Assign median runtime based on genres
        for idx in cleaned_df[zero_runtime_mask].index:
            movie_genres = cleaned_df.loc[idx, 'genres_list']
            if movie_genres:
                # Get medians for all genres of this movie
                genre_runtimes = [genre_medians.get(genre, np.nan) for genre in movie_genres]
                # Use the median of available genre medians
                valid_runtimes = [rt for rt in genre_runtimes if not np.isnan(rt)]
                if valid_runtimes:
                    median_runtime = np.median(valid_runtimes)
                    cleaned_df.loc[idx, 'runtime'] = median_runtime

    return cleaned_df


def merge_duplicate_movies(df, ratings_df=None, links_df=None):
    """
    Merge duplicate movie entries by:
    - Averaging popularity values
    - Calculating vote_count from ratings_df using links_df mapping
    - Keeping first occurrence for other fields

    Args:
        df: DataFrame with movies_metadata (uses 'id' which is TMDb ID)
        ratings_df: DataFrame with ratings data (uses 'movieId' from links)
        links_df: DataFrame to map tmdbId (= movies.id) to movieId (= ratings.movieId)

    Returns:
        cleaned_df: DataFrame with duplicates merged
    """
    print(f"\nChecking for duplicate movie IDs...")
    initial_count = len(df)

    # Convert popularity to numeric, handling any string concatenation issues
    df['popularity'] = pd.to_numeric(df['popularity'], errors='coerce')

    # Calculate actual vote counts from ratings if available
    # Mapping: movies.id (tmdbId) -> links.tmdbId -> links.movieId -> ratings.movieId
    vote_counts = {}
    links_df_converted = None

    if ratings_df is not None and links_df is not None:
        print("Calculating vote counts from ratings data...")
        # Count ratings by movieId
        ratings_counts = ratings_df.groupby('movieId').size().to_dict()
        print(f"Calculated {len(ratings_counts)} vote counts from ratings")

        # Convert links DataFrame types once
        print("Converting links DataFrame types...")
        links_df_converted = links_df.copy()
        links_df_converted['tmdbId'] = pd.to_numeric(links_df_converted['tmdbId'], errors='coerce')
        links_df_converted['movieId'] = pd.to_numeric(links_df_converted['movieId'], errors='coerce')
        links_df_converted = links_df_converted.dropna(subset=['tmdbId', 'movieId'])
        links_df_converted['tmdbId'] = links_df_converted['tmdbId'].astype(int)
        links_df_converted['movieId'] = links_df_converted['movieId'].astype(int)

        # Debug: Check if 105045 is in the converted links
        test_match = links_df_converted[links_df_converted['tmdbId'] == 105045]
        print(f"DEBUG: tmdbId 105045 found in links? {len(test_match)} matches")
        if len(test_match) > 0:
            print(f"  Sample rows: {test_match[['movieId', 'tmdbId']].head()}")

        # Map tmdbId to movieId using links
        print("Mapping tmdbId to movieId using links...")
        for _, link_row in links_df_converted.iterrows():
            tmdb_id = link_row['tmdbId']
            movie_id = link_row['movieId']

            if movie_id in ratings_counts:
                # Map tmdbId (which is movies.id) to vote count
                # If multiple movieIds map to same tmdbId, sum the ratings
                if tmdb_id in vote_counts:
                    vote_counts[tmdb_id] += ratings_counts[movie_id]
                else:
                    vote_counts[tmdb_id] = ratings_counts[movie_id]

        print(f"Mapped vote counts for {len(vote_counts)} movies (tmdbId -> movieId -> ratings)")

    # Find duplicates
    duplicates = df[df.duplicated(subset=['id'], keep=False)]

    if len(duplicates) == 0:
        print("No duplicates found!")
        return df

    duplicate_ids = duplicates['id'].unique()
    print(f"Found {len(duplicate_ids)} unique movie IDs with duplicates")

    # Group by id and merge
    merged_rows = []

    for movie_id_str in duplicate_ids:
        # Convert to int for vote_counts lookup, but keep string for DataFrame filtering
        movie_id = int(float(movie_id_str))
        dup_rows = df[df['id'] == movie_id_str].copy()

        if len(dup_rows) > 1:
            # Take first row as base
            merged = dup_rows.iloc[0].copy()

            # Debug
            if movie_id == 105045:
                print(f"  DEBUG 105045: Processing duplicate, {len(dup_rows)} rows")
                print(f"  DEBUG 105045: vote_count in vote_counts? {movie_id in vote_counts}")

            # Average popularity across all duplicates (only if they differ)
            popularity_values = dup_rows['popularity'].dropna()
            if len(popularity_values) > 1:
                unique_popularity = popularity_values.unique()
                if len(unique_popularity) > 1:
                    # Values differ, so average them
                    merged['popularity'] = popularity_values.mean()
                    print(f"  ID {movie_id}: Averaged popularity from {list(popularity_values)} to {merged['popularity']:.6f}")
                # else: values are identical, keep first (already in merged)

            # Handle vote_count: only use ratings if the duplicate rows have conflicting values
            vote_count_values = dup_rows['vote_count'].dropna()
            if len(vote_count_values) > 1:
                unique_vote_counts = vote_count_values.unique()
                if len(unique_vote_counts) > 1:
                    # Values differ - calculate from ratings if available, otherwise average
                    if movie_id in vote_counts:
                        old_vote_counts = list(vote_count_values)
                        merged['vote_count'] = vote_counts[movie_id]
                        print(f"  ID {movie_id}: Set vote_count to {merged['vote_count']} from ratings (CSV had conflicting {old_vote_counts})")
                    else:
                        # No ratings data available, so average the conflicting values
                        merged['vote_count'] = int(vote_count_values.mean())
                        print(f"  ID {movie_id}: Averaged vote_count from {list(vote_count_values)} to {merged['vote_count']} (no ratings data)")
                # else: values are identical, keep first (already in merged)

            merged_rows.append(merged)

    # Remove all duplicate rows and add merged ones
    df_no_dupes = df[~df['id'].isin(duplicate_ids)].copy()
    merged_df = pd.concat([df_no_dupes, pd.DataFrame(merged_rows)], ignore_index=True)

    print(f"\n✓ Merged {initial_count - len(merged_df)} duplicate entries")
    print(f"Final movie count: {len(merged_df)}")

    return merged_df


def merge_duplicate_credits(df):
    """
    Merge duplicate credit entries by:
    - Merging crew lists (keeping unique crew members)
    - Merging cast lists (keeping unique cast members)
    - Showing differences when crew differs

    Args:
        df: DataFrame with credits data

    Returns:
        cleaned_df: DataFrame with duplicates merged
    """
    print(f"\nChecking for duplicate credit IDs...")
    initial_count = len(df)

    # Find duplicates
    duplicates = df[df.duplicated(subset=['id'], keep=False)]

    if len(duplicates) == 0:
        print("No duplicates found!")
        return df

    duplicate_ids = duplicates['id'].unique()
    print(f"Found {len(duplicate_ids)} unique credit IDs with duplicates")

    # Group by id and merge
    merged_rows = []

    for credit_id in duplicate_ids:
        dup_rows = df[df['id'] == credit_id].copy()

        if len(dup_rows) > 1:
            # Parse cast and crew
            all_cast = []
            all_crew = []

            for idx, row in dup_rows.iterrows():
                cast = ast.literal_eval(row['cast']) if pd.notna(row['cast']) and row['cast'] != '' else []
                crew = ast.literal_eval(row['crew']) if pd.notna(row['crew']) and row['crew'] != '' else []
                all_cast.extend(cast)
                all_crew.extend(crew)

            # Check if crew differs between occurrences and show detailed diff
            crew_lists = []
            for idx, row in dup_rows.iterrows():
                crew = ast.literal_eval(row['crew']) if pd.notna(row['crew']) and row['crew'] != '' else []
                crew_lists.append(crew)

            if len(set(str(sorted(c, key=lambda x: x.get('credit_id', ''))) for c in crew_lists)) > 1:
                print(f"\n  🎬 ID {credit_id}: Crew differs between occurrences")

                # Show crew members in each occurrence
                for i, crew_list in enumerate(crew_lists, 1):
                    print(f"    Occurrence {i}: {len(crew_list)} crew members")

                # Show the differences
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

            # Remove duplicates from cast and crew based on credit_id
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

            # Create merged row
            merged = dup_rows.iloc[0].copy()
            merged['cast'] = str(unique_cast)
            merged['crew'] = str(unique_crew)

            if len(unique_crew) > len(ast.literal_eval(dup_rows.iloc[0]['crew']) if pd.notna(dup_rows.iloc[0]['crew']) and dup_rows.iloc[0]['crew'] != '' else []):
                print(f"  ID {credit_id}: Merged crew from {[len(ast.literal_eval(row['crew']) if pd.notna(row['crew']) and row['crew'] != '' else []) for _, row in dup_rows.iterrows()]} to {len(unique_crew)} unique members")

            merged_rows.append(merged)

    # Remove all duplicate rows and add merged ones
    df_no_dupes = df[~df['id'].isin(duplicate_ids)].copy()
    merged_df = pd.concat([df_no_dupes, pd.DataFrame(merged_rows)], ignore_index=True)

    print(f"\n✓ Merged {initial_count - len(merged_df)} duplicate entries")
    print(f"Final credits count: {len(merged_df)}")

    return merged_df

def clean_credits_crew(df):
    """
    Clean credits by removing rows with empty or missing crew.

    Args:
        df: DataFrame with credits data

    Returns:
        cleaned_df: DataFrame with rows having valid crew
    """
    original_rows = len(df)
    print(f"Original credits rows: {original_rows}")

    # Parse crew to check if empty
    df['crew_parsed'] = df['crew'].apply(lambda x: ast.literal_eval(x) if pd.notnull(x) and x != '' else [])

    # Keep rows where crew is not null, not empty string, and not empty array
    valid_crew_mask = ~((df['crew'].isnull() | (df['crew'] == '') | (df['crew_parsed'].apply(lambda x: len(x) == 0))))

    cleaned_df = df[valid_crew_mask].copy()
    cleaned_df = cleaned_df.drop(columns=['crew_parsed'])  # Remove temporary column

    rows_removed = original_rows - len(cleaned_df)
    print(f"Rows removed: {rows_removed}")
    print(f"Rows after cleaning: {len(cleaned_df)}")

    return cleaned_df

def clean_keywords(df):
    """
    Clean keywords by removing rows with empty or missing keywords,
    and remove duplicates.

    Args:
        df: DataFrame with keywords data

    Returns:
        cleaned_df: DataFrame with rows having valid keywords and no duplicates
    """
    original_rows = len(df)
    print(f"Original keywords rows: {original_rows}")

    # Parse keywords to check if empty
    df['keywords_parsed'] = df['keywords'].apply(lambda x: ast.literal_eval(x) if pd.notnull(x) and x != '' else [])

    # Keep rows where keywords is not null, not empty string, and not empty array
    valid_keywords_mask = ~((df['keywords'].isnull() | (df['keywords'] == '') | (df['keywords_parsed'].apply(lambda x: len(x) == 0))))

    cleaned_df = df[valid_keywords_mask].copy()
    cleaned_df = cleaned_df.drop(columns=['keywords_parsed'])  # Remove temporary column

    rows_removed = original_rows - len(cleaned_df)
    print(f"Rows removed: {rows_removed}")
    print(f"Rows after cleaning: {len(cleaned_df)}")

    print(f"After removing empty keywords: {len(cleaned_df)} rows (removed {len(df) - len(cleaned_df)} rows)")

    # Check for duplicates
    initial_count = len(cleaned_df)
    duplicate_mask = cleaned_df.duplicated(subset=['id'], keep='first')
    if duplicate_mask.sum() > 0:
        print(f"Found {duplicate_mask.sum()} duplicate keyword entries")
        cleaned_df = cleaned_df[~duplicate_mask]
        print(f"Removed {initial_count - len(cleaned_df)} duplicate entries")

    print(f"Final keywords rows: {len(cleaned_df)}")
    return cleaned_df


def clean_links(df):
    """
    Remove duplicate links based on movieId.

    Args:
        df: DataFrame with links data

    Returns:
        cleaned_df: DataFrame with no duplicates
    """
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
    """
    Remove duplicate ratings based on (userId, movieId).
    Keep the most recent rating (highest timestamp).

    Args:
        df: DataFrame with ratings data

    Returns:
        cleaned_df: DataFrame with no duplicates
    """
    print(f"Original ratings rows: {len(df)}")

    initial_count = len(df)
    # Sort by timestamp descending to keep most recent
    df_sorted = df.sort_values('timestamp', ascending=False)

    # Remove duplicates, keeping first (most recent due to sorting)
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
    """
    Save cleaned credits dataframe to CSV.

    Args:
        df: Cleaned DataFrame
        output_path: Path to save the CSV
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False)
    print(f"Cleaned credits saved to {output_file}")

def save_cleaned_keywords(df, output_path='data/movies_cleaned/keywords_cleaned.csv'):
    """
    Save cleaned keywords dataframe to CSV.

    Args:
        df: Cleaned DataFrame
        output_path: Path to save the CSV
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False)
    print(f"Cleaned keywords saved to {output_file}")

def save_cleaned_movies(df, output_path='data/movies_cleaned/movies_metadata_cleaned.csv'):
    """
    Save cleaned movies dataframe to CSV.

    Args:
        df: Cleaned DataFrame
        output_path: Path to save the CSV
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False)
    print(f"Cleaned data saved to {output_file}")

if __name__ == '__main__':
    # Define data files
    data_files = {
        'movies_metadata.csv': 'data/movies_cleaned/movies_metadata_cleaned.csv',
        'credits.csv': 'data/movies_cleaned/credits_cleaned.csv',
        'keywords.csv': 'data/movies_cleaned/keywords_cleaned.csv',
        'links.csv': 'data/movies_cleaned/links_cleaned.csv',
        'links_small.csv': 'data/movies_cleaned/links_small_cleaned.csv',
        'ratings.csv': 'data/movies_cleaned/ratings_cleaned.csv',
        'ratings_small.csv': 'data/movies_cleaned/ratings_small_cleaned.csv'
    }

    # First, load ratings and links to calculate accurate vote counts
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

    # Load and clean each dataset
    for input_file, output_file in data_files.items():
        data_path = Path('data/movies') / input_file
        if data_path.exists():
            print(f"\n{'='*60}")
            print(f"Processing {input_file}...")
            print('='*60)
            df = pd.read_csv(data_path, low_memory=False)
            original_rows = len(df)

            if input_file == 'movies_metadata.csv':
                # First clean runtime and filter released movies
                cleaned_df = clean_movies_runtime(df)
                # Then merge duplicates (pass ratings and links for accurate vote counts)
                cleaned_df = merge_duplicate_movies(cleaned_df, ratings_df, links_df)
                save_cleaned_movies(cleaned_df, output_file)
            elif input_file == 'credits.csv':
                # Clean crew and merge duplicates
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
                # For other files, just save as is (no cleaning specified)
                print(f"Original {input_file} rows: {original_rows}")
                print(f"Rows removed: 0")
                print(f"Rows after cleaning: {original_rows}")
                output_path = Path(output_file)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                df.to_csv(output_path, index=False)
                print(f"Data saved to {output_path} (no cleaning applied)")
        else:
            print(f"Data file not found: {data_path}")
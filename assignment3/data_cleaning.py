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
    Clean keywords by removing rows with empty or missing keywords.

    Args:
        df: DataFrame with keywords data

    Returns:
        cleaned_df: DataFrame with rows having valid keywords
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

    # Load and clean each dataset
    for input_file, output_file in data_files.items():
        data_path = Path('data/movies') / input_file
        if data_path.exists():
            print(f"\nProcessing {input_file}...")
            df = pd.read_csv(data_path, low_memory=False)
            original_rows = len(df)

            if input_file == 'movies_metadata.csv':
                cleaned_df = clean_movies_runtime(df)
                save_cleaned_movies(cleaned_df, output_file)
            elif input_file == 'credits.csv':
                cleaned_df = clean_credits_crew(df)
                save_cleaned_credits(cleaned_df, output_file)
            elif input_file == 'keywords.csv':
                cleaned_df = clean_keywords(df)
                save_cleaned_keywords(cleaned_df, output_file)
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
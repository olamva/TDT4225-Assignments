from pathlib import Path

import pandas as pd
from tabulate import tabulate

data_dir = Path('../data/movies')

def analyze_ratings(df):
    print("=== Ratings Analysis ===")
    print(f"Number of rows: {len(df)}")
    print(f"Number of columns: {len(df.columns)}")

    dtypes_table = [[col, str(dtype)] for col, dtype in df.dtypes.items()]
    print("\nFeatures and types:")
    print(tabulate(dtypes_table, headers=['Feature', 'Type'], tablefmt='grid'))

    print("\nMissing values:")
    missing = df.isnull().sum()

    if 'userId' in df.columns:
        user_missing = df['userId'].isnull().sum()
        print(f"Rows with missing values for users (userId): {user_missing}")

    if 'movieId' in df.columns:
        movie_missing = df['movieId'].isnull().sum()
        print(f"Rows with missing values for movies (movieId): {movie_missing}")

    print("\nDetailed missing values and zero values:")
    for col in df.columns:
        null_count = missing[col]
        zero_count = (df[col] == 0).sum() if df[col].dtype in ['int64', 'float64'] else 0
        if null_count > 0 or zero_count > 0:
            print(f"  {col}: {null_count} null ({null_count/len(df)*100:.2f}%), {zero_count} zero ({zero_count/len(df)*100:.2f}%)")
    if missing.sum() == 0 and all((df[col] != 0).sum() == len(df) for col in df.columns if df[col].dtype in ['int64', 'float64']):
        print("  No missing or zero values found.")

    analyze_ratings_specific(df)

def analyze_ratings_specific(df):
    print("\n=== Ratings Specific Analysis ===")
    try:
        missing = df.isnull().sum()
        total_missing = missing.sum()
        print(f"Rows with missing values: {total_missing}")

        # Count invalid/zero IDs if columns exist
        if 'userId' in df.columns:
            invalid_user_ids = df['userId'].isnull().sum() + (df['userId'] == 0).sum()
        else:
            invalid_user_ids = 0

        if 'movieId' in df.columns:
            invalid_movie_ids = df['movieId'].isnull().sum() + (df['movieId'] == 0).sum()
        else:
            invalid_movie_ids = 0

        total_invalid_ids = invalid_user_ids + invalid_movie_ids
        print(f"Rows with invalid userId: {invalid_user_ids}")
        print(f"Rows with invalid movieId: {invalid_movie_ids}")
        print(f"Total rows with invalid IDs: {total_invalid_ids}")

        # New: number of unique users (if present)
        if 'userId' in df.columns:
            unique_users = df['userId'].nunique(dropna=True)
            print(f"Number of unique users: {unique_users}")

        # New: rating statistics (min, max, median, mean) if rating column exists
        # Accept common column names: 'rating'
        rating_col = None
        for candidate in ('rating', 'Rating'):
            if candidate in df.columns:
                rating_col = candidate
                break

        if rating_col:
            ratings = pd.to_numeric(df[rating_col], errors='coerce').dropna()
            if len(ratings) > 0:
                r_min = ratings.min()
                r_max = ratings.max()
                r_median = ratings.median()
                r_mean = ratings.mean()
                print("\nRating statistics:")
                print(f"  min: {r_min:.3f}")
                print(f"  max: {r_max:.3f}")
                print(f"  median: {r_median:.3f}")
                print(f"  mean: {r_mean:.3f}")
            else:
                print("No valid rating values found (all missing or non-numeric).")
        else:
            print("No rating column found to compute statistics.")

    except Exception as e:
        print(f"Error analyzing ratings: {e}")

def main():
    data_path = data_dir / 'ratings.csv'
    if data_path.exists():
        df = pd.read_csv(data_path, low_memory=False)
        analyze_ratings(df)
    else:
        print(f"Data file not found: {data_path}")

if __name__ == '__main__':
    main()
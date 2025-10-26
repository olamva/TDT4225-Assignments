from pathlib import Path

import pandas as pd
from tabulate import tabulate

# Set up paths
data_dir = Path('../data/movies')

def analyze_ratings(df, is_small=False):
    """Analyze ratings.csv or ratings_small.csv"""
    dataset_name = "Ratings Small" if is_small else "Ratings"
    print(f"=== {dataset_name} Analysis ===")
    print(f"Number of rows: {len(df)}")
    print(f"Number of columns: {len(df.columns)}")

    # Features and types in a table
    dtypes_table = [[col, str(dtype)] for col, dtype in df.dtypes.items()]
    print("\nFeatures and types:")
    print(tabulate(dtypes_table, headers=['Feature', 'Type'], tablefmt='grid'))

    print("\nMissing values:")
    missing = df.isnull().sum()
    
    # Check for missing values in specific columns
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

    # Specific analysis for ratings
    analyze_ratings_specific(df, is_small)

def analyze_ratings_specific(df, is_small=False):
    """Specific analysis for ratings datasets"""
    print("\n=== Ratings Specific Analysis ===")
    try:
        # Missing values
        missing = df.isnull().sum()
        total_missing = missing.sum()
        print(f"Rows with missing values: {total_missing}")

        # Invalid IDs (null or 0)
        invalid_user_ids = df['userId'].isnull().sum() + (df['userId'] == 0).sum()
        invalid_movie_ids = df['movieId'].isnull().sum() + (df['movieId'] == 0).sum()
        total_invalid_ids = invalid_user_ids + invalid_movie_ids
        print(f"Rows with invalid userId: {invalid_user_ids}")
        print(f"Rows with invalid movieId: {invalid_movie_ids}")
        print(f"Total rows with invalid IDs: {total_invalid_ids}")

        # For ratings_small.csv, calculate average rating
        if is_small:
            avg_rating = df['rating'].mean()
            print(f"Average rating: {avg_rating:.2f}")

    except Exception as e:
        print(f"Error analyzing ratings: {e}")

def main():
    # Analyze ratings.csv
    data_path = data_dir / 'ratings.csv'
    if data_path.exists():
        df = pd.read_csv(data_path, low_memory=False)
        analyze_ratings(df, is_small=False)
    else:
        print(f"Data file not found: {data_path}")

if __name__ == '__main__':
    main()
from pathlib import Path

import pandas as pd
from tabulate import tabulate

data_dir = Path('../data/movies')

def analyze_ratings_small(df):
    print("=== Ratings Small Analysis ===")

    dtypes_table = [[col, str(dtype)] for col, dtype in df.dtypes.items()]
    print("Features and types:")
    print(tabulate(dtypes_table, headers=['Feature', 'Type'], tablefmt='grid'))

    print("\nMissing values and zero values:")
    missing = df.isnull().sum()
    for col in df.columns:
        null_count = missing[col]
        zero_count = (df[col] == 0).sum() if df[col].dtype in ['int64', 'float64'] else 0
        if null_count > 0 or zero_count > 0:
            print(f"  {col}: {null_count} null ({null_count/len(df)*100:.2f}%), {zero_count} zero ({zero_count/len(df)*100:.2f}%)")
    if missing.sum() == 0 and all((df[col] != 0).sum() == len(df) for col in df.columns if df[col].dtype in ['int64', 'float64']):
        print("  No missing or zero values found.")

    analyze_ratings_small_specific(df)

def analyze_ratings_small_specific(df):
    print("\n=== Ratings Small Specific Analysis ===")
    try:
        missing = df.isnull().sum()
        total_missing = missing.sum()
        print(f"Rows with missing values: {total_missing}")

        invalid_user_ids = df['userId'].isnull().sum() + (df['userId'] == 0).sum()
        invalid_movie_ids = df['movieId'].isnull().sum() + (df['movieId'] == 0).sum()
        total_invalid_ids = invalid_user_ids + invalid_movie_ids
        print(f"Rows with invalid userId: {invalid_user_ids}")
        print(f"Rows with invalid movieId: {invalid_movie_ids}")
        print(f"Total rows with invalid IDs: {total_invalid_ids}")

        avg_rating = df['rating'].mean()
        print(f"Average rating: {avg_rating:.2f}")

    except Exception as e:
        print(f"Error analyzing ratings_small: {e}")

def main():
    data_path = data_dir / 'ratings_small.csv'
    if data_path.exists():
        df = pd.read_csv(data_path, low_memory=False)
        analyze_ratings_small(df)
    else:
        print(f"Data file not found: {data_path}")

if __name__ == '__main__':
    main()
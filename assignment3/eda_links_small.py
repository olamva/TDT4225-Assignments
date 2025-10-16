from pathlib import Path

import pandas as pd
from tabulate import tabulate

# Set up paths
data_dir = Path('data/movies')

def analyze_links_small(df):
    """Analyze links_small.csv"""
    print("=== Links Small Analysis ===")

    # Attributes and types
    dtypes_table = [[col, str(dtype)] for col, dtype in df.dtypes.items()]
    print("Attributes and types:")
    print(tabulate(dtypes_table, headers=['Attribute', 'Type'], tablefmt='grid'))

    print("\nMissing values and zero values:")
    missing = df.isnull().sum()
    for col in df.columns:
        null_count = missing[col]
        zero_count = (df[col] == 0).sum() if df[col].dtype in ['int64', 'float64'] else 0
        if null_count > 0 or zero_count > 0:
            print(f"  {col}: {null_count} null ({null_count/len(df)*100:.2f}%), {zero_count} zero ({zero_count/len(df)*100:.2f}%)")
    if missing.sum() == 0 and all((df[col] != 0).sum() == len(df) for col in df.columns if df[col].dtype in ['int64', 'float64']):
        print("  No missing or zero values found.")

    # Specific analysis for links_small.csv
    analyze_links_small_specific(df)

def analyze_links_small_specific(df):
    """Specific analysis for links_small.csv"""
    print("\n=== Links Small Specific Analysis ===")

    # Missing values
    missing = df.isnull().sum()
    total_missing = missing.sum()
    print(f"Rows with missing values: {total_missing}")

    if total_missing > 0:
        print("Missing values by column:")
        for col in df.columns:
            if missing[col] > 0:
                print(f"  {col}: {missing[col]} ({missing[col]/len(df)*100:.2f}%)")

    # Check for missing/null imdbId
    if 'imdbId' in df.columns:
        null_imdb = df['imdbId'].isnull().sum()
        empty_imdb = (df['imdbId'] == '').sum() if df['imdbId'].dtype == 'object' else 0
        total_invalid_imdb = null_imdb + empty_imdb
        print(f"Rows with missing/null imdbId: {total_invalid_imdb}")
        if total_invalid_imdb > 0:
            print(f"  Null imdbId: {null_imdb}")
            print(f"  Empty imdbId: {empty_imdb}")

def main():
    # Analyze links_small.csv
    data_path = data_dir / 'links_small.csv'
    if data_path.exists():
        df = pd.read_csv(data_path, low_memory=False)
        analyze_links_small(df)
    else:
        print(f"Data file not found: {data_path}")

if __name__ == '__main__':
    main()
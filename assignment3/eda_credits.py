import ast
from pathlib import Path

import pandas as pd
from tabulate import tabulate

# Set up paths
data_dir = Path('data/movies')

def analyze_credits(df):
    """Analyze credits.csv"""
    print("=== Credits Analysis ===")

    # Features and types in a table
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

    # Specific analysis for credits.csv
    analyze_credits_specific(df)

def analyze_credits_specific(df):
    """Specific analysis for credits.csv"""
    print("\n=== Credits Specific Analysis ===")
    try:
        df['cast_parsed'] = df['cast'].apply(lambda x: ast.literal_eval(x) if pd.notnull(x) and x != '' else [])
        df['crew_parsed'] = df['crew'].apply(lambda x: ast.literal_eval(x) if pd.notnull(x) and x != '' else [])

        null_cast = df['cast'].isnull().sum() + (df['cast'] == '').sum()
        empty_cast = df['cast_parsed'].apply(lambda x: len(x) == 0).sum()
        total_cast_issues = null_cast + empty_cast

        null_crew = df['crew'].isnull().sum() + (df['crew'] == '').sum()
        empty_crew = df['crew_parsed'].apply(lambda x: len(x) == 0).sum()
        total_crew_issues = null_crew + empty_crew

        print(f"Cast: {total_cast_issues} rows with null or empty arrays")
        print(f"Crew: {total_crew_issues} rows with null or empty arrays")

    except Exception as e:
        print(f"Error analyzing cast/crew: {e}")

def main():
    # Analyze credits.csv
    data_path = data_dir / 'credits.csv'
    if data_path.exists():
        df = pd.read_csv(data_path, low_memory=False)
        analyze_credits(df)
    else:
        print(f"Data file not found: {data_path}")

if __name__ == '__main__':
    main()
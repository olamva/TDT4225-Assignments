import ast
from pathlib import Path

import pandas as pd
from tabulate import tabulate

data_dir = Path('../data/movies')

def analyze_keywords(df):
    print("=== Keywords Analysis ===")
    print(f"Number of rows: {len(df)}")
    print(f"Number of columns: {len(df.columns)}")

    dtypes_table = [[col, str(dtype)] for col, dtype in df.dtypes.items()]
    print("\nAttributes and types:")
    print(tabulate(dtypes_table, headers=['Attribute', 'Type'], tablefmt='grid'))

    print("\nMissing values and zero values:")
    missing = df.isnull().sum()

    id_missing = df['id'].isnull().sum()
    print(f"Rows with missing values for movies (id): {id_missing}")

    keywords_missing = df['keywords'].isnull().sum()
    keywords_empty = (df['keywords'] == '').sum()
    total_keywords_missing = keywords_missing + keywords_empty
    print(f"Rows with missing values for keywords: {total_keywords_missing} ({keywords_missing} null + {keywords_empty} empty)")

    print()
    for col in df.columns:
        null_count = missing[col]
        zero_count = (df[col] == 0).sum() if df[col].dtype in ['int64', 'float64'] else 0
        if null_count > 0 or zero_count > 0:
            print(f"  {col}: {null_count} null ({null_count/len(df)*100:.2f}%), {zero_count} zero ({zero_count/len(df)*100:.2f}%)")
    if missing.sum() == 0 and all((df[col] != 0).sum() == len(df) for col in df.columns if df[col].dtype in ['int64', 'float64']):
        print("  No missing or zero values found.")

    analyze_keywords_specific(df)

def analyze_keywords_specific(df):
    print("\n=== Keywords Specific Analysis ===")
    try:
        df['keywords_parsed'] = df['keywords'].apply(lambda x: ast.literal_eval(x) if pd.notnull(x) and x != '' else [])

        null_keywords = df['keywords'].isnull().sum() + (df['keywords'] == '').sum()
        empty_keywords = df['keywords_parsed'].apply(lambda x: len(x) == 0).sum()
        total_issues = null_keywords + empty_keywords

        print(f"Keywords: {total_issues} rows with null or empty arrays")

    except Exception as e:
        print(f"Error analyzing keywords: {e}")

def main():
    data_path = data_dir / 'keywords.csv'
    if data_path.exists():
        df = pd.read_csv(data_path, low_memory=False)
        analyze_keywords(df)
    else:
        print(f"Data file not found: {data_path}")

if __name__ == '__main__':
    main()
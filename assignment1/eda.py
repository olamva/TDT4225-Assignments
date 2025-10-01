import numpy as np
import pandas as pd
from data_loader import get_missing_data_rows, load_porto_data

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 50)

# Load the data using the data loader
df = load_porto_data()

# Display basic information about the dataset
print("=== PORTO TAXI DATASET ANALYSIS ===\n")

print("Dataset shape:", df.shape)
print("\nColumn names:")
for i, col in enumerate(df.columns, 1):
    print(f"{i}. {col}")

print("\n=== DATA TYPES ===")
print(df.dtypes)

print("\n=== FIRST FEW ROWS ===")
print(df.head())

print("\n=== SUMMARY STATISTICS (NUMERIC COLUMNS) ===")
numeric_cols = df.select_dtypes(include=[np.number]).columns
if len(numeric_cols) > 0:
    print(df[numeric_cols].describe().round(2))

print("\n=== CALL_TYPE DISTRIBUTION ===")
if 'CALL_TYPE' in df.columns:
    print(df['CALL_TYPE'].value_counts())
    print("\nCall Type Meanings:")
    print("A: Trip dispatched from central")
    print("B: Trip requested from taxi stand")
    print("C: Trip hailed on street")

print("\n=== DAYTYPE DISTRIBUTION ===")
if 'DAYTYPE' in df.columns:
    print(df['DAYTYPE'].value_counts())
    print("\nDay Type Meanings:")
    print("A: Normal day (workday/weekend)")
    print("B: Holiday or special day")
    print("C: Day before holiday")

print("\n=== MISSING DATA FLAG ===")
if 'MISSING_DATA' in df.columns:
    print(df['MISSING_DATA'].value_counts())

print("\n=== NULL VALUES PER COLUMN ===")
print(df.isnull().sum())

print("\n=== SAMPLE POLYLINE DATA ===")
if 'POLYLINE' in df.columns:
    # Show first non-empty polyline
    for idx, polyline in df['POLYLINE'].items():
        if pd.notna(polyline) and polyline != '[]':
            print(f"Trip {idx} polyline (first 100 chars): {str(polyline)[:100]}...")
            break

print("\n=== ROWS WITH MISSING DATA (MISSING_DATA = True) ===")
if 'MISSING_DATA' in df.columns:
    missing_data_rows = get_missing_data_rows(df)
    print(f"Total rows with missing data: {len(missing_data_rows)}")
    print(f"Percentage of rows with missing data: {(len(missing_data_rows) / len(df) * 100):.2f}%")

    if len(missing_data_rows) > 0:
        print("\nFirst 10 rows with missing data:")
        print(missing_data_rows.head(10))

        print("\nSample of POLYLINE data for missing data rows:")
        for idx, (index, row) in enumerate(missing_data_rows.head(5).iterrows()):
            print(f"Row {index} - TRIP_ID: {row.get('TRIP_ID', 'N/A')}, POLYLINE: {str(row.get('POLYLINE', 'N/A'))[:100]}...")
    else:
        print("No rows found with MISSING_DATA = True")
else:
    print("MISSING_DATA column not found in dataset")
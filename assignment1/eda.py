import numpy as np
import pandas as pd

csv = "./porto.csv"
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 50)

# Reading the data into a pandas DataFrame
# Note: TIMESTAMP needs to be converted from Unix time to datetime
df = pd.read_csv(csv, nrows=10)

# Convert TIMESTAMP from Unix time to datetime
if 'TIMESTAMP' in df.columns:
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], unit='s')

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
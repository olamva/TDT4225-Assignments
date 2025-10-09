import re

import pandas as pd

from utils.data_loader import load_porto_data


def fast_parse_polyline_len(s):
    """
    Fast polyline length parsing using regex instead of ast.literal_eval.
    Counts the number of coordinate pairs by counting commas and brackets.
    """
    if pd.isna(s) or s == "" or s == "[]":
        return 0
    try:
        # Count coordinate pairs by finding pattern [number,number]
        # Each coordinate pair has format [longitude,latitude]
        pairs = re.findall(r'\[[-+]?\d*\.?\d+,[-+]?\d*\.?\d+\]', str(s))
        return len(pairs)
    except Exception:
        return 0


def find_global_min_max_days():
    """
    Find the dates with global minimum and maximum trip counts.

    Returns:
        tuple: (min_date, max_date, min_count, max_count)
    """
    # Load the data
    print("Loading Porto taxi data to find global min/max days...")
    df = load_porto_data(csv_path="data/original/porto.csv", pickle_path="data/original/porto_data.pkl", verbose=True)

    # Convert TIMESTAMP to datetime if needed
    if "TIMESTAMP" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["TIMESTAMP"]):
        df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], unit="s", errors="coerce")

    if "TIMESTAMP" not in df.columns:
        raise ValueError("TIMESTAMP column not found in dataset")

    # Extract date from timestamp
    df["date"] = df["TIMESTAMP"].dt.date

    # Count trips per day
    daily_trips = df.groupby("date").size().reset_index(name="trip_count")

    # Find global minimum and maximum
    min_day = daily_trips.loc[daily_trips["trip_count"].idxmin()]
    max_day = daily_trips.loc[daily_trips["trip_count"].idxmax()]

    return min_day["date"], max_day["date"], min_day["trip_count"], max_day["trip_count"]


def clean_dataset(save_to_file=True, output_csv="data/cleaned/cleaned_porto_data.csv", output_pickle="data/cleaned/cleaned_porto_data.pkl"):
    """
    Remove rows with missing data, rows from global min/max trip days, and short duration trips (≤ 30 seconds).

    Args:
        save_to_file (bool): Whether to save the cleaned dataset
        output_csv (str): Output CSV filename
        output_pickle (str): Output pickle filename

    Returns:
        pandas.DataFrame: Cleaned dataset
    """
    print("=== CLEANING PORTO TAXI DATASET ===\n")

    # Load the original data
    print("Loading original dataset...")
    df = load_porto_data(csv_path="data/original/porto.csv", pickle_path="data/original/porto_data.pkl", verbose=True)

    original_rows = len(df)
    print(f"Original dataset: {original_rows:,} rows")

    # Convert TIMESTAMP to datetime if needed
    if "TIMESTAMP" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["TIMESTAMP"]):
        df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], unit="s", errors="coerce")

    # Extract date for filtering
    df["date"] = df["TIMESTAMP"].dt.date

    # Step 1: Remove rows with MISSING_DATA = True
    if "MISSING_DATA" in df.columns:
        missing_data_rows = df["MISSING_DATA"].sum()
        print(f"\n📊 MISSING DATA ANALYSIS:")
        print(f"Rows with MISSING_DATA = True: {missing_data_rows:,}")
        print(f"Percentage of missing data: {(missing_data_rows/original_rows)*100:.2f}%")

        df_cleaned = df[df["MISSING_DATA"] == False].copy()
        rows_after_missing_removal = len(df_cleaned)
        print(f"Rows after removing missing data: {rows_after_missing_removal:,}")
        print(f"Rows removed: {original_rows - rows_after_missing_removal:,}")
    else:
        print("\n⚠️  MISSING_DATA column not found, skipping missing data removal")
        df_cleaned = df.copy()
        rows_after_missing_removal = len(df_cleaned)

    # Step 2: Find global min/max days
    print(f"\n🔍 FINDING GLOBAL MIN/MAX DAYS:")
    min_date, max_date, min_count, max_count = find_global_min_max_days()

    print(f"Global minimum day: {min_date} ({min_count:,} trips)")
    print(f"Global maximum day: {max_date} ({max_count:,} trips)")

    # Step 3: Remove rows from global min/max days
    print(f"\n🗑️  REMOVING GLOBAL MIN/MAX DAYS:")

    # Count rows before removal
    min_day_rows = len(df_cleaned[df_cleaned["date"] == min_date])
    max_day_rows = len(df_cleaned[df_cleaned["date"] == max_date])

    print(f"Rows from minimum day ({min_date}): {min_day_rows:,}")
    print(f"Rows from maximum day ({max_date}): {max_day_rows:,}")

    # Remove the rows
    df_cleaned = df_cleaned[
        (df_cleaned["date"] != min_date) &
        (df_cleaned["date"] != max_date)
    ].copy()

    rows_after_minmax_removal = len(df_cleaned)
    total_minmax_removed = rows_after_missing_removal - rows_after_minmax_removal

    print(f"Rows removed from min/max days: {total_minmax_removed:,}")
    print(f"Rows after min/max removal: {rows_after_minmax_removal:,}")

    # Step 4: Remove rows with duration ≤ 30 seconds
    print(f"\n⏱️  REMOVING SHORT DURATION TRIPS (≤ 30 seconds):")

    if "POLYLINE" in df_cleaned.columns:
        # Calculate duration from polyline data
        print("Calculating trip durations from polyline data...")
        df_cleaned["poly_len"] = df_cleaned["POLYLINE"].apply(fast_parse_polyline_len)
        df_cleaned["duration_sec"] = (df_cleaned["poly_len"].clip(lower=1) - 1) * 15

        # Count rows with short duration
        short_duration_rows = len(df_cleaned[df_cleaned["duration_sec"] <= 30])
        print(f"Rows with duration ≤ 30 seconds: {short_duration_rows:,}")
        print(f"Percentage of short duration trips: {(short_duration_rows/rows_after_minmax_removal)*100:.2f}%")

        # Remove short duration trips
        df_cleaned = df_cleaned[df_cleaned["duration_sec"] > 30].copy()

        # Drop temporary calculation columns
        df_cleaned = df_cleaned.drop(["poly_len", "duration_sec"], axis=1)

        rows_after_duration_removal = len(df_cleaned)
        duration_removed = rows_after_minmax_removal - rows_after_duration_removal

        print(f"Rows removed (short duration): {duration_removed:,}")
        print(f"Final dataset: {rows_after_duration_removal:,} rows")
    else:
        print("⚠️  POLYLINE column not found, skipping duration filtering")
        rows_after_duration_removal = rows_after_minmax_removal
        duration_removed = 0

    # Drop the temporary date column
    df_cleaned = df_cleaned.drop("date", axis=1)

    # Summary
    print(f"\n📈 CLEANING SUMMARY:")
    print(f"Original rows: {original_rows:,}")
    print(f"Rows removed (missing data): {original_rows - rows_after_missing_removal:,}")
    print(f"Rows removed (min/max days): {total_minmax_removed:,}")
    print(f"Rows removed (short duration ≤ 30s): {duration_removed:,}")
    print(f"Final rows: {rows_after_duration_removal:,}")
    print(f"Total reduction: {original_rows - rows_after_duration_removal:,} rows ({((original_rows - rows_after_duration_removal)/original_rows)*100:.2f}%)")

    # Data quality check
    print(f"\n✅ DATA QUALITY CHECK:")
    if "MISSING_DATA" in df_cleaned.columns:
        remaining_missing = df_cleaned["MISSING_DATA"].sum()
        print(f"Remaining rows with MISSING_DATA = True: {remaining_missing}")

    # Check date range
    if "TIMESTAMP" in df_cleaned.columns:
        df_cleaned_temp = df_cleaned.copy()
        df_cleaned_temp["date"] = df_cleaned_temp["TIMESTAMP"].dt.date
        date_range = {
            "earliest": df_cleaned_temp["date"].min(),
            "latest": df_cleaned_temp["date"].max(),
            "unique_days": df_cleaned_temp["date"].nunique()
        }
        print(f"Date range: {date_range['earliest']} to {date_range['latest']}")
        print(f"Unique days remaining: {date_range['unique_days']}")

    # Save the cleaned dataset
    if save_to_file:
        print(f"\n💾 SAVING CLEANED DATASET:")

        # Save as CSV
        df_cleaned.to_csv(output_csv, index=False)
        print(f"CSV saved: {output_csv}")

        # Save as pickle for faster loading
        df_cleaned.to_pickle(output_pickle)
        print(f"Pickle saved: {output_pickle}")

        # Show file sizes
        import os
        csv_size = os.path.getsize(output_csv) / (1024**2)  # MB
        pickle_size = os.path.getsize(output_pickle) / (1024**2)  # MB
        print(f"CSV size: {csv_size:.1f} MB")
        print(f"Pickle size: {pickle_size:.1f} MB")

    return df_cleaned


def validate_cleaning(cleaned_df=None):
    """
    Validate that the cleaning was done correctly.

    Args:
        cleaned_df (pandas.DataFrame): Cleaned dataset (optional, will load if not provided)

    Returns:
        dict: Validation results
    """
    if cleaned_df is None:
        # Try to load cleaned data
        try:
            cleaned_df = pd.read_pickle("data/cleaned/cleaned_porto_data.pkl")
        except FileNotFoundError:
            print("Cleaned dataset not found. Run clean_dataset() first.")
            return None

    print("\n🔍 VALIDATION CHECK:")

    validation = {
        "total_rows": len(cleaned_df),
        "has_missing_data": False,
        "missing_data_count": 0,
        "date_range": None
    }

    # Check for missing data
    if "MISSING_DATA" in cleaned_df.columns:
        missing_count = cleaned_df["MISSING_DATA"].sum()
        validation["missing_data_count"] = missing_count
        validation["has_missing_data"] = missing_count > 0

        if missing_count == 0:
            print("✅ No rows with MISSING_DATA = True found")
        else:
            print(f"❌ Found {missing_count} rows with MISSING_DATA = True")

    # Check date range
    if "TIMESTAMP" in cleaned_df.columns:
        df_temp = cleaned_df.copy()
        if not pd.api.types.is_datetime64_any_dtype(df_temp["TIMESTAMP"]):
            df_temp["TIMESTAMP"] = pd.to_datetime(df_temp["TIMESTAMP"], unit="s", errors="coerce")

        df_temp["date"] = df_temp["TIMESTAMP"].dt.date
        validation["date_range"] = {
            "earliest": df_temp["date"].min(),
            "latest": df_temp["date"].max(),
            "unique_days": df_temp["date"].nunique()
        }

        print(f"📅 Date range: {validation['date_range']['earliest']} to {validation['date_range']['latest']}")
        print(f"📊 Unique days: {validation['date_range']['unique_days']}")

    return validation


def main():
    """
    Main function to clean the dataset.
    """
    try:
        # Clean the dataset
        cleaned_df = clean_dataset(save_to_file=True)

        # Validate the cleaning
        validate_cleaning(cleaned_df)

        print("\n🎉 DATASET CLEANING COMPLETED!")

        return cleaned_df

    except Exception as e:
        print(f"❌ Error during cleaning: {e}")
        return None


if __name__ == "__main__":
    main()
# visualize_porto_optimized.py
# Optimized version of Porto Taxi visualization with significant performance improvements

import os
import re

import matplotlib.pyplot as plt
import pandas as pd
from data_loader import load_porto_data


def ensure_matplotlib_backend():
    """
    Unngå problemer på headless-miljøer ved å bruke en ikke-interaktiv backend.
    """
    import matplotlib
    if os.environ.get("MPLBACKEND") is None:
        matplotlib.use("Agg")


def fast_parse_polyline_len(s):
    """
    Much faster polyline length parsing using regex instead of ast.literal_eval.
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





def create_time_based_plots(df, outdir):
    """
    Create time-based visualizations that don't require polyline parsing.
    """
    plots_created = []

    # Convert timestamp if needed
    if "TIMESTAMP" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["TIMESTAMP"]):
        df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], unit="s", errors="coerce")

    if "TIMESTAMP" in df.columns:
        df["hour"] = df["TIMESTAMP"].dt.hour
        df["date"] = df["TIMESTAMP"].dt.date

        # Plot 1: Antall turer per time på døgnet
        if "hour" in df.columns:
            counts_by_hour = df["hour"].value_counts().sort_index()
            plt.figure(figsize=(10, 6))
            counts_by_hour.plot(kind="bar")
            plt.title("Number of trips per hour of day")
            plt.xlabel("Hour (0-23)")
            plt.ylabel("Number of trips")
            plt.tight_layout()
            plt.savefig(os.path.join(outdir, "trips_per_hour.png"))
            plt.close()
            plots_created.append("trips_per_hour.png")

        # Plot 2: Daglige turer
        if "date" in df.columns:
            per_day = df.groupby("date").size().sort_index()
            plt.figure(figsize=(12, 6))
            per_day.plot(kind="line")
            plt.title("Number of trips per day")
            plt.xlabel("Date")
            plt.ylabel("Number of trips")
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(os.path.join(outdir, "trips_per_day.png"))
            plt.close()
            plots_created.append("trips_per_day.png")

    return plots_created


def create_categorical_plots(df, outdir):
    """
    Create categorical visualizations.
    """
    plots_created = []

    # Plot: Fordeling av CALL_TYPE
    if "CALL_TYPE" in df.columns:
        call_counts = df["CALL_TYPE"].value_counts()
        plt.figure(figsize=(8, 6))
        call_counts.plot(kind="bar")
        plt.title("Distribution of CALL_TYPE")
        plt.xlabel("CALL_TYPE")
        plt.ylabel("Count")
        plt.tight_layout()
        plt.savefig(os.path.join(outdir, "call_type_distribution.png"))
        plt.close()
        plots_created.append("call_type_distribution.png")

    # Plot: Manglende data
    if "MISSING_DATA" in df.columns:
        miss_counts = df["MISSING_DATA"].value_counts(dropna=False)
        plt.figure(figsize=(8, 6))
        miss_counts.plot(kind="bar")
        plt.title("MISSING_DATA distribution")
        plt.xlabel("MISSING_DATA (False/True)")
        plt.ylabel("Number of rows")
        plt.tight_layout()
        plt.savefig(os.path.join(outdir, "missing_data_distribution.png"))
        plt.close()
        plots_created.append("missing_data_distribution.png")

    return plots_created


def create_duration_plot(df, outdir):
    """
    Create duration visualization using optimized polyline parsing.
    Processes the entire dataset.
    """
    plots_created = []

    if "POLYLINE" in df.columns:
        print(f"Processing polyline data for duration analysis on full dataset...")

        # Use optimized polyline parsing on full dataset
        print("Parsing polyline lengths (this may take a moment)...")
        df["poly_len"] = df["POLYLINE"].apply(fast_parse_polyline_len)

        # Calculate duration
        df["duration_sec"] = (df["poly_len"].clip(lower=1) - 1) * 15

        # Filter out extreme values for cleaner visualization
        cutoff = df["duration_sec"].quantile(0.95)
        duration_data = df.loc[df["duration_sec"] <= cutoff, "duration_sec"]

        plt.figure(figsize=(10, 6))
        duration_data.hist(bins=40, edgecolor='black', alpha=0.7)
        plt.title(f"Distribution of trip duration (sec, capped at 95th percentile)\n"
                 f"Based on {len(df)} rows")
        plt.xlabel("Duration (seconds)")
        plt.ylabel("Number of trips")
        plt.tight_layout()
        plt.savefig(os.path.join(outdir, "duration_histogram.png"))
        plt.close()
        plots_created.append("duration_histogram.png")

        print(f"Duration analysis completed using full dataset ({len(df)} rows)")

    return plots_created


def main():
    ensure_matplotlib_backend()

    print("=== PORTO TAXI VISUALIZATION (OPTIMIZED) ===")

    # 1) Load cleaned dataset
    print("Loading cleaned dataset...")
    try:
        # Try to load cleaned data first (faster)
        df = pd.read_pickle("./data/cleaned/cleaned_porto_data.pkl")
        print("Loaded cleaned dataset from pickle cache")
    except FileNotFoundError:
        # Fallback to CSV if pickle doesn't exist
        try:
            df = pd.read_csv("./data/cleaned/cleaned_porto_data.csv")
            print("Loaded cleaned dataset from CSV")
            # Convert TIMESTAMP if needed
            if "TIMESTAMP" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["TIMESTAMP"]):
                df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], unit="s", errors="coerce")
        except FileNotFoundError:
            print("❌ Cleaned dataset not found! Please run clean_dataset.py first.")
            print("Falling back to original dataset...")
            df = load_porto_data(csv_path="./data/original/porto.csv", pickle_path="./data/original/porto_data.pkl", verbose=True)

    print(f"Loaded dataset with {len(df)} rows and {len(df.columns)} columns")

    # 2) Create output directory
    outdir = "./figures"
    os.makedirs(outdir, exist_ok=True)

    # 3) Create visualizations in groups for better performance
    all_plots = []

    print("Creating time-based visualizations...")
    all_plots.extend(create_time_based_plots(df, outdir))

    print("Creating categorical visualizations...")
    all_plots.extend(create_categorical_plots(df, outdir))

    print("Creating duration visualization...")
    all_plots.extend(create_duration_plot(df, outdir))

    # 4) Summary
    print("\n=== VISUALIZATIONS COMPLETED ===")
    print(f"Output directory: {outdir}")
    for plot in sorted(all_plots):
        print(f"  • {plot}")

    # 5) Dataset summary
    print("\n=== DATASET SUMMARY ===")
    basic_info = {
        "processed_rows": len(df),
        "total_columns": len(df.columns),
        "columns": list(df.columns),
        "memory_usage_MB": round(df.memory_usage(deep=True).sum() / 1024**2, 2),
    }

    for key, value in basic_info.items():
        if key == "columns":
            print(f"{key}: {', '.join(value)}")
        else:
            print(f"{key}: {value}")

    if "MISSING_DATA" in df.columns:
        missing_pct = (df["MISSING_DATA"].sum() / len(df)) * 100
        print(f"missing_data_percentage: {missing_pct:.2f}%")


if __name__ == "__main__":
    main()
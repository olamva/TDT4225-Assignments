"""
Porto Taxi Dataset Loader

This module provides functions to load and cache the Porto taxi dataset
with proper data type conversions and caching for performance.
"""

import os

import pandas as pd


def load_porto_data(csv_path="porto.csv", pickle_path="./data/original/porto_data.pkl", verbose=True):
    """
    Load Porto taxi data with caching support.

    Args:
        csv_path (str): Path to the CSV file
        pickle_path (str): Path to the pickle cache file
        verbose (bool): Whether to print loading messages

    Returns:
        pandas.DataFrame: The loaded and processed Porto taxi dataset
    """

    # Check if pickle file exists and is newer than CSV
    if os.path.exists(pickle_path) and os.path.exists(csv_path):
        if os.path.getmtime(pickle_path) > os.path.getmtime(csv_path):
            if verbose:
                print("Loading cached data from pickle file...")
            return pd.read_pickle(pickle_path)

    # Load from CSV if pickle doesn't exist or is older
    if verbose:
        print("Reading CSV file and creating cache...")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    df = pd.read_csv(csv_path)

    # Convert TIMESTAMP from Unix time to datetime
    if 'TIMESTAMP' in df.columns:
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], unit='s')

    # Save to pickle for faster loading next time
    try:
        df.to_pickle(pickle_path)
        if verbose:
            print("Data cached to pickle file for faster future loading.")
    except Exception as e:
        if verbose:
            print(f"Warning: Could not save pickle cache: {e}")

    return df


def get_missing_data_rows(df):
    """
    Get all rows where MISSING_DATA is True.

    Args:
        df (pandas.DataFrame): The Porto taxi dataset

    Returns:
        pandas.DataFrame: Subset of data where MISSING_DATA is True
    """
    if 'MISSING_DATA' not in df.columns:
        raise ValueError("MISSING_DATA column not found in dataset")

    return df[df['MISSING_DATA'] == True]


def get_dataset_info(df):
    """
    Get basic information about the dataset.

    Args:
        df (pandas.DataFrame): The Porto taxi dataset

    Returns:
        dict: Dictionary containing dataset information
    """
    info = {
        'shape': df.shape,
        'columns': list(df.columns),
        'dtypes': df.dtypes.to_dict(),
        'null_counts': df.isnull().sum().to_dict(),
        'total_rows': len(df)
    }

    # Add missing data statistics if column exists
    if 'MISSING_DATA' in df.columns:
        missing_count = len(df[df['MISSING_DATA'] == True])
        info['missing_data_count'] = missing_count
        info['missing_data_percentage'] = (missing_count / len(df)) * 100

    return info


def clear_cache(pickle_path="./data/original/porto_data.pkl"):
    """
    Clear the pickle cache file.

    Args:
        pickle_path (str): Path to the pickle cache file
    """
    if os.path.exists(pickle_path):
        os.remove(pickle_path)
        print(f"Cache file {pickle_path} removed.")
    else:
        print(f"Cache file {pickle_path} does not exist.")


if __name__ == "__main__":
    # Example usage
    df = load_porto_data()
    info = get_dataset_info(df)

    print("=== DATASET INFO ===")
    print(f"Shape: {info['shape']}")
    print(f"Total rows: {info['total_rows']}")
    if 'missing_data_count' in info:
        print(f"Rows with missing data: {info['missing_data_count']} ({info['missing_data_percentage']:.2f}%)")
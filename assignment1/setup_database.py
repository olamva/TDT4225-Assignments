import sys

import mysql.connector as mysql


def setup_database(db_name="testdb"):
    """Create the specified database if it doesn't exist"""
    try:
        # Connect to MySQL server without specifying a database
        connection = mysql.connect(
            host="localhost",
            user="root",
            password="secret",
            port=3306
        )
        cursor = connection.cursor()

        # Create database if it doesn't exist
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        print(f"Database '{db_name}' created successfully (or already exists)")

        # Close the connection
        cursor.close()
        connection.close()

    except Exception as e:
        print(f"ERROR: Failed to create database: {e}")

if __name__ == "__main__":
    # Check if a database name was provided as command line argument
    if len(sys.argv) > 1:
        db_name = sys.argv[1]
    else:
        db_name = "testdb"

    setup_database(db_name)
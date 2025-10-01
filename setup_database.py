import mysql.connector as mysql


def setup_database():
    """Create the testdb database if it doesn't exist"""
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
        cursor.execute("CREATE DATABASE IF NOT EXISTS testdb")
        print("Database 'testdb' created successfully (or already exists)")

        # Close the connection
        cursor.close()
        connection.close()

    except Exception as e:
        print(f"ERROR: Failed to create database: {e}")

if __name__ == "__main__":
    setup_database()
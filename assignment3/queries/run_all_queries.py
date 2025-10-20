"""
Run all queries sequentially
"""

import os
import sys

# Add parent directory to path to import queries
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import query1
import query2
import query3
import query4
import query5
import query6
import query7
import query8
import query9
import query10


def main():
    print("\n" + "="*100)
    print("RUNNING ALL QUERIES FOR ASSIGNMENT 3")
    print("="*100 + "\n")

    queries = [
        ("Query 1", query1.run_query),
        ("Query 2", query2.run_query),
        ("Query 3", query3.run_query),
        ("Query 4", query4.run_query),
        ("Query 5", query5.run_query),
        ("Query 6", query6.run_query),
        ("Query 7", query7.run_query),
        ("Query 8", query8.run_query),
        ("Query 9", query9.run_query),
        ("Query 10", query10.run_query),
    ]

    for name, query_func in queries:
        try:
            print(f"\nExecuting {name}...")
            query_func()
            print(f"✓ {name} completed successfully\n")
        except Exception as e:
            print(f"✗ {name} failed: {e}\n")
            import traceback
            traceback.print_exc()

    print("\n" + "="*100)
    print("ALL QUERIES COMPLETED")
    print("="*100 + "\n")


if __name__ == '__main__':
    main()

import traceback

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
        query1.run_query,
        query2.run_query,
        query3.run_query,
        query4.run_query,
        query5.run_query,
        query6.run_query,
        query7.run_query,
        query8.run_query,
        query9.run_query,
        query10.run_query
    ]

    for query_func in queries:
        try:
            query_func()
        except Exception:
            traceback.print_exc()

    print("\n" + "="*100)
    print("ALL QUERIES COMPLETED")
    print("="*100 + "\n")


if __name__ == '__main__':
    main()

"""
Query 7: Top 20 movies matching "noir" or "neo-noir" in overview/tagline
Filter: vote_count ≥ 50, sorted by vote_average (desc)
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json

from DbConnector import DbConnector


def run_query():
    db_connector = DbConnector(DATABASE='assignment3')
    db = db_connector.db

    # Note: Using regex instead of text search for more reliable matching
    # Text search was missing some results due to indexing/stemming issues
    pipeline = [
        # Match movies with "noir" in overview or tagline (case-insensitive)
        {'$match': {
            '$or': [
                {'overview': {'$regex': 'noir', '$options': 'i'}},
                {'tagline': {'$regex': 'noir', '$options': 'i'}}
            ],
            'vote_count': {'$gte': 50}
        }},

        # Extract year from release_date
        {'$addFields': {
            'year': {
                '$cond': {
                    'if': {'$and': [
                        {'$ne': ['$release_date', None]},
                        {'$gte': [{'$strLenCP': '$release_date'}, 4]}
                    ]},
                    'then': {'$substr': ['$release_date', 0, 4]},
                    'else': 'N/A'
                }
            }
        }},

        # Sort by vote_average descending
        {'$sort': {'vote_average': -1}},

        # Limit to top 20
        {'$limit': 20},

        # Format output
        {'$project': {
            '_id': 0,
            'title': 1,
            'year': 1,
            'vote_average': {'$round': ['$vote_average', 2]},
            'vote_count': 1,
            'overview': 1
        }}
    ]

    results = list(db.movies.aggregate(pipeline))

    # Print results
    print("\n" + "="*100)
    print("Query 7: Top 20 'Noir' Movies (vote_count ≥ 50) by Vote Average")
    print("="*100)

    if results:
        print(f"\n{'Rank':<5} {'Title':<50} {'Year':<6} {'Vote Avg':<10} {'Vote Count':<12}")
        print("-"*100)
        for i, result in enumerate(results, 1):
            print(f"{i:<5} {result['title'][:48]:<50} {result['year']:<6} "
                  f"{result['vote_average']:<10.2f} {result['vote_count']:<12}")
    else:
        print("No results found.")

    print("="*100 + "\n")

    db_connector.close_connection()
    return results


if __name__ == '__main__':
    results = run_query()

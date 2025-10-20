"""
Query 9: Top 10 original languages for non-English movies with US production
Report count and one example title per language
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json

from DbConnector import DbConnector


def run_query():
    db_connector = DbConnector(DATABASE='assignment3')
    db = db_connector.db

    pipeline = [
        # Filter: original_language ≠ "en"
        {'$match': {
            'original_language': {'$ne': 'en', '$exists': True},
            '$or': [
                # US production company
                {'production_companies.name': {'$regex': 'United States', '$options': 'i'}},
                # US production country
                {'production_countries.name': 'United States of America'}
            ]
        }},

        # Group by original language
        {'$group': {
            '_id': '$original_language',
            'count': {'$sum': 1},
            'example_title': {'$first': '$title'}
        }},

        # Sort by count descending
        {'$sort': {'count': -1}},

        # Limit to top 10
        {'$limit': 10},

        # Format output
        {'$project': {
            '_id': 0,
            'language': '$_id',
            'count': 1,
            'example_title': 1
        }}
    ]

    results = list(db.movies.aggregate(pipeline))

    # Print results
    print("\n" + "="*100)
    print("Query 9: Top 10 Original Languages (Non-English, US Production)")
    print("="*100)

    if results:
        print(f"\n{'Rank':<5} {'Language':<15} {'Count':<10} {'Example Title':<60}")
        print("-"*100)
        for i, result in enumerate(results, 1):
            print(f"{i:<5} {result['language']:<15} {result['count']:<10} {result['example_title'][:58]:<60}")
    else:
        print("No results found.")

    print("="*100 + "\n")

    db_connector.close_connection()
    return results


if __name__ == '__main__':
    results = run_query()

    # Optionally save to JSON
    with open('queries/results/query9_results.json', 'w') as f:
        json.dump(results, f, indent=2)

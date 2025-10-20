"""
Query 6: Proportion of female cast in top 5 billed positions, aggregated by decade
Sorted by average female proportion (desc)
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
        # Lookup movie details
        {'$lookup': {
            'from': 'movies',
            'localField': 'id',
            'foreignField': 'id',
            'as': 'movie'
        }},

        {'$unwind': '$movie'},

        # Filter movies with release_date
        {'$match': {
            'movie.release_date': {'$ne': None, '$exists': True}
        }},

        # Unwind cast and filter top 5 by order
        {'$unwind': '$cast'},

        {'$match': {
            'cast.order': {'$lte': 4},  # 0-4 = top 5
            'cast.gender': {'$in': [1, 2]}  # 1 = female, 2 = male
        }},

        # Extract decade
        {'$addFields': {
            'year': {
                '$toInt': {
                    '$substr': ['$movie.release_date', 0, 4]
                }
            }
        }},

        {'$addFields': {
            'decade': {
                '$concat': [
                    {'$toString': {'$multiply': [{'$floor': {'$divide': ['$year', 10]}}, 10]}},
                    's'
                ]
            }
        }},

        # Group by movie and decade
        {'$group': {
            '_id': {
                'movie_id': '$id',
                'decade': '$decade'
            },
            'female_count': {
                '$sum': {
                    '$cond': [{'$eq': ['$cast.gender', 1]}, 1, 0]
                }
            },
            'total_count': {'$sum': 1}
        }},

        # Calculate female proportion per movie
        {'$addFields': {
            'female_proportion': {
                '$divide': ['$female_count', '$total_count']
            }
        }},

        # Group by decade
        {'$group': {
            '_id': '$_id.decade',
            'movie_count': {'$sum': 1},
            'avg_female_proportion': {'$avg': '$female_proportion'}
        }},

        # Sort by average female proportion descending
        {'$sort': {'avg_female_proportion': -1}},

        # Format output
        {'$project': {
            '_id': 0,
            'decade': '$_id',
            'movie_count': 1,
            'avg_female_proportion': {'$round': ['$avg_female_proportion', 4]}
        }}
    ]

    results = list(db.credits.aggregate(pipeline))

    # Print results
    print("\n" + "="*80)
    print("Query 6: Average Female Proportion in Top 5 Cast by Decade")
    print("="*80)

    if results:
        print(f"\n{'Decade':<15} {'Movie Count':<15} {'Avg Female Proportion':<25}")
        print("-"*80)
        for result in results:
            percentage = result['avg_female_proportion'] * 100
            print(f"{result['decade']:<15} {result['movie_count']:<15} "
                  f"{result['avg_female_proportion']:<10.4f} ({percentage:.2f}%)")
    else:
        print("No results found.")

    print("="*80 + "\n")

    db_connector.close_connection()
    return results


if __name__ == '__main__':
    results = run_query()

    # Optionally save to JSON
    with open('queries/results/query6_results.json', 'w') as f:
        json.dump(results, f, indent=2)

"""
Query 5: Median runtime and movie count by decade and primary genre
Sorted by decade then median runtime (desc)
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
        # Filter movies with release_date and runtime
        {'$match': {
            'release_date': {'$ne': None, '$exists': True},
            'runtime': {'$ne': None, '$gt': 0},
            'genres_list': {'$exists': True, '$ne': []}
        }},

        # Extract decade from release_date and primary genre
        {'$addFields': {
            'year': {
                '$toInt': {
                    '$substr': ['$release_date', 0, 4]
                }
            },
            'primary_genre': {'$arrayElemAt': ['$genres_list', 0]}
        }},

        # Calculate decade
        {'$addFields': {
            'decade': {
                '$concat': [
                    {'$toString': {'$multiply': [{'$floor': {'$divide': ['$year', 10]}}, 10]}},
                    's'
                ]
            }
        }},

        # Group by decade and primary genre
        {'$group': {
            '_id': {
                'decade': '$decade',
                'primary_genre': '$primary_genre'
            },
            'movie_count': {'$sum': 1},
            'runtimes': {'$push': '$runtime'}
        }},

        # Calculate median runtime
        {'$addFields': {
            'sorted_runtimes': {'$sortArray': {'input': '$runtimes', 'sortBy': 1}}
        }},

        {'$addFields': {
            'median_runtime': {
                '$let': {
                    'vars': {
                        'size': {'$size': '$sorted_runtimes'},
                        'mid': {'$floor': {'$divide': [{'$size': '$sorted_runtimes'}, 2]}}
                    },
                    'in': {
                        '$cond': [
                            {'$eq': [{'$mod': ['$$size', 2]}, 0]},
                            {'$avg': [
                                {'$arrayElemAt': ['$sorted_runtimes', '$$mid']},
                                {'$arrayElemAt': ['$sorted_runtimes', {'$subtract': ['$$mid', 1]}]}
                            ]},
                            {'$arrayElemAt': ['$sorted_runtimes', '$$mid']}
                        ]
                    }
                }
            }
        }},

        # Sort by decade, then median runtime descending
        {'$sort': {'_id.decade': 1, 'median_runtime': -1}},

        # Format output
        {'$project': {
            '_id': 0,
            'decade': '$_id.decade',
            'primary_genre': '$_id.primary_genre',
            'movie_count': 1,
            'median_runtime': {'$round': ['$median_runtime', 1]}
        }}
    ]

    results = list(db.movies.aggregate(pipeline))

    # Print results
    print("\n" + "="*80)
    print("Query 5: Median Runtime and Movie Count by Decade and Primary Genre")
    print("="*80)

    if results:
        current_decade = None
        for result in results:
            if current_decade != result['decade']:
                if current_decade is not None:
                    print()
                current_decade = result['decade']
                print(f"\n{current_decade}")
                print("-"*80)
                print(f"{'Primary Genre':<25} {'Movies':<10} {'Median Runtime (min)':<20}")
                print("-"*80)

            print(f"{result['primary_genre']:<25} {result['movie_count']:<10} {result['median_runtime']:<20.1f}")
    else:
        print("No results found.")

    print("="*80 + "\n")

    db_connector.close_connection()
    return results


if __name__ == '__main__':
    results = run_query()

    # Optionally save to JSON
    with open('queries/results/query5_results.json', 'w') as f:
        json.dump(results, f, indent=2)

"""
Query 1: Top 10 directors (≥5 movies) with highest median revenue
Also report movie count and mean vote_average
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
        # Unwind crew array
        {'$unwind': '$crew'},

        # Filter for directors only
        {'$match': {'crew.job': 'Director'}},

        # Lookup movie details
        {'$lookup': {
            'from': 'movies',
            'localField': 'id',
            'foreignField': 'id',
            'as': 'movie'
        }},

        # Unwind movie array
        {'$unwind': '$movie'},

        # Group by director
        {'$group': {
            '_id': {
                'director_id': '$crew.id',
                'director_name': '$crew.name'
            },
            'movie_count': {'$sum': 1},
            'revenues': {'$push': '$movie.revenue'},
            'vote_averages': {'$avg': '$movie.vote_average'}
        }},

        # Filter directors with ≥ 5 movies
        {'$match': {'movie_count': {'$gte': 5}}},

        # Calculate median revenue
        {'$addFields': {
            'sorted_revenues': {'$sortArray': {'input': '$revenues', 'sortBy': 1}},
        }},

        {'$addFields': {
            'median_revenue': {
                '$let': {
                    'vars': {
                        'size': {'$size': '$sorted_revenues'},
                        'mid': {'$floor': {'$divide': [{'$size': '$sorted_revenues'}, 2]}}
                    },
                    'in': {
                        '$cond': [
                            {'$eq': [{'$mod': ['$$size', 2]}, 0]},
                            # Even: average of two middle values
                            {'$avg': [
                                {'$arrayElemAt': ['$sorted_revenues', '$$mid']},
                                {'$arrayElemAt': ['$sorted_revenues', {'$subtract': ['$$mid', 1]}]}
                            ]},
                            # Odd: middle value
                            {'$arrayElemAt': ['$sorted_revenues', '$$mid']}
                        ]
                    }
                }
            }
        }},

        # Sort by median revenue descending
        {'$sort': {'median_revenue': -1}},

        # Limit to top 10
        {'$limit': 10},

        # Format output
        {'$project': {
            '_id': 0,
            'director_name': '$_id.director_name',
            'director_id': '$_id.director_id',
            'movie_count': 1,
            'median_revenue': {'$round': ['$median_revenue', 2]},
            'mean_vote_average': {'$round': ['$vote_averages', 2]}
        }}
    ]

    results = list(db.credits.aggregate(pipeline))

    # Print results
    print("\n" + "="*80)
    print("Query 1: Top 10 Directors (≥5 movies) with Highest Median Revenue")
    print("="*80)

    if results:
        print(f"\n{'Rank':<5} {'Director':<30} {'Movies':<8} {'Median Revenue':<18} {'Mean Vote Avg':<15}")
        print("-"*80)
        for i, result in enumerate(results, 1):
            print(f"{i:<5} {result['director_name']:<30} {result['movie_count']:<8} "
                  f"${result['median_revenue']:>15,.2f} {result['mean_vote_average']:>15.2f}")
    else:
        print("No results found.")

    print("="*80 + "\n")

    db_connector.close_connection()
    return results


if __name__ == '__main__':
    results = run_query()

    # Optionally save to JSON
    with open('queries/results/query1_results.json', 'w') as f:
        json.dump(results, f, indent=2)

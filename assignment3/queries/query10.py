"""
Query 10: User statistics - ratings count, variance, and genre diversity
Top 10 most genre-diverse users and top 10 highest-variance users (≥20 ratings)
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json

from DbConnector import DbConnector


def run_query():
    db_connector = DbConnector(DATABASE='assignment3')
    db = db_connector.db

    # Pipeline for all user statistics
    pipeline = [
        # Lookup movie details to get genres
        {'$lookup': {
            'from': 'movies',
            'localField': 'movieId',
            'foreignField': 'id',
            'as': 'movie'
        }},

        {'$unwind': '$movie'},

        # Unwind genres for genre counting
        {'$unwind': {
            'path': '$movie.genres_list',
            'preserveNullAndEmptyArrays': True
        }},

        # Group by user
        {'$group': {
            '_id': '$userId',
            'ratings_count': {'$sum': 1},
            'ratings': {'$push': '$rating'},
            'genres': {'$addToSet': '$movie.genres_list'}
        }},

        # Filter users with ≥20 ratings
        {'$match': {'ratings_count': {'$gte': 20}}},

        # Calculate variance and genre diversity
        {'$addFields': {
            'mean_rating': {'$avg': '$ratings'},
            'genre_count': {'$size': '$genres'}
        }},

        # Calculate population variance
        {'$addFields': {
            'variance': {
                '$divide': [
                    {
                        '$reduce': {
                            'input': '$ratings',
                            'initialValue': 0,
                            'in': {
                                '$add': [
                                    '$$value',
                                    {'$pow': [
                                        {'$subtract': ['$$this', '$mean_rating']},
                                        2
                                    ]}
                                ]
                            }
                        }
                    },
                    '$ratings_count'
                ]
            }
        }},

        # Format output
        {'$project': {
            '_id': 0,
            'userId': '$_id',
            'ratings_count': 1,
            'variance': {'$round': ['$variance', 4]},
            'genre_count': 1
        }}
    ]

    all_users = list(db.ratings.aggregate(pipeline))

    # Sort for top genre-diverse users
    genre_diverse = sorted(all_users, key=lambda x: x['genre_count'], reverse=True)[:10]

    # Sort for top variance users
    high_variance = sorted(all_users, key=lambda x: x['variance'], reverse=True)[:10]

    # Print results
    print("\n" + "="*100)
    print("Query 10: User Rating Statistics (≥20 ratings)")
    print("="*100)

    print("\n" + "="*100)
    print("TOP 10 MOST GENRE-DIVERSE USERS")
    print("="*100)

    if genre_diverse:
        print(f"\n{'Rank':<5} {'User ID':<15} {'Ratings Count':<15} {'Genre Count':<15} {'Variance':<15}")
        print("-"*100)
        for i, user in enumerate(genre_diverse, 1):
            print(f"{i:<5} {user['userId']:<15} {user['ratings_count']:<15} "
                  f"{user['genre_count']:<15} {user['variance']:<15.4f}")

    print("\n" + "="*100)
    print("TOP 10 HIGHEST-VARIANCE USERS")
    print("="*100)

    if high_variance:
        print(f"\n{'Rank':<5} {'User ID':<15} {'Ratings Count':<15} {'Variance':<15} {'Genre Count':<15}")
        print("-"*100)
        for i, user in enumerate(high_variance, 1):
            print(f"{i:<5} {user['userId']:<15} {user['ratings_count']:<15} "
                  f"{user['variance']:<15.4f} {user['genre_count']:<15}")

    print("="*100 + "\n")

    db_connector.close_connection()

    return {
        'genre_diverse': genre_diverse,
        'high_variance': high_variance
    }


if __name__ == '__main__':
    results = run_query()

    # Optionally save to JSON
    with open('queries/results/query10_results.json', 'w') as f:
        json.dump(results, f, indent=2)

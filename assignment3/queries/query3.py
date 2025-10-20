"""
Query 3: Top 10 actors (≥10 credited movies) with widest genre breadth
Report distinct genres count and up to 5 example genres
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
        # Unwind cast array
        {'$unwind': '$cast'},

        # Lookup movie details
        {'$lookup': {
            'from': 'movies',
            'localField': 'id',
            'foreignField': 'id',
            'as': 'movie'
        }},

        {'$unwind': '$movie'},

        # Unwind genres_list
        {'$unwind': '$movie.genres_list'},

        # Group by actor
        {'$group': {
            '_id': {
                'actor_id': '$cast.id',
                'actor_name': '$cast.name'
            },
            'movie_count': {'$sum': 1},
            'genres': {'$addToSet': '$movie.genres_list'}
        }},

        # Filter actors with ≥10 movies
        {'$match': {'movie_count': {'$gte': 10}}},

        # Calculate genre breadth
        {'$addFields': {
            'genre_count': {'$size': '$genres'},
            'example_genres': {'$slice': ['$genres', 5]}
        }},

        # Sort by genre count descending
        {'$sort': {'genre_count': -1, 'movie_count': -1}},

        # Limit to top 10
        {'$limit': 10},

        # Format output
        {'$project': {
            '_id': 0,
            'actor': '$_id.actor_name',
            'movie_count': 1,
            'genre_count': 1,
            'example_genres': 1
        }}
    ]

    results = list(db.credits.aggregate(pipeline))

    # Print results
    print("\n" + "="*120)
    print("Query 3: Top 10 Actors (≥10 movies) with Widest Genre Breadth")
    print("="*120)

    if results:
        print(f"\n{'Rank':<5} {'Actor':<30} {'Movies':<8} {'Genres':<8} {'Example Genres':<60}")
        print("-"*120)
        for i, result in enumerate(results, 1):
            genres_str = ', '.join(result['example_genres'])
            print(f"{i:<5} {result['actor']:<30} {result['movie_count']:<8} "
                  f"{result['genre_count']:<8} {genres_str:<60}")
    else:
        print("No results found.")

    print("="*120 + "\n")

    db_connector.close_connection()
    return results


if __name__ == '__main__':
    results = run_query()

    # Optionally save to JSON
    with open('queries/results/query3_results.json', 'w') as f:
        json.dump(results, f, indent=2)

"""
Query 2: Actor pairs that co-starred in ≥3 movies
Report co-appearances count and average movie vote_average
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json

from DbConnector import DbConnector


def run_query():
    db_connector = DbConnector(DATABASE='assignment3')
    db = db_connector.db

    # Step 1: Get all movies with their cast and vote_average
    pipeline_movies = [
        {'$lookup': {
            'from': 'movies',
            'localField': 'id',
            'foreignField': 'id',
            'as': 'movie'
        }},
        {'$unwind': '$movie'},
        {'$project': {
            'movie_id': '$id',
            'vote_average': '$movie.vote_average',
            'cast': {
                '$map': {
                    'input': '$cast',
                    'as': 'actor',
                    'in': {
                        'id': '$$actor.id',
                        'name': '$$actor.name'
                    }
                }
            }
        }}
    ]

    movies_with_cast = list(db.credits.aggregate(pipeline_movies))

    # Step 2: Generate actor pairs in Python
    from collections import defaultdict
    from itertools import combinations

    pair_data = defaultdict(lambda: {'count': 0, 'votes': []})

    for movie in movies_with_cast:
        cast = movie.get('cast', [])
        vote_avg = movie.get('vote_average', 0)

        # Generate all unique pairs of actors in this movie
        for actor1, actor2 in combinations(cast, 2):
            # Ensure consistent ordering (smaller id first)
            if actor1['id'] < actor2['id']:
                pair_key = (actor1['id'], actor1['name'], actor2['id'], actor2['name'])
            else:
                pair_key = (actor2['id'], actor2['name'], actor1['id'], actor1['name'])

            pair_data[pair_key]['count'] += 1
            pair_data[pair_key]['votes'].append(vote_avg)

    # Step 3: Filter pairs with ≥3 co-appearances and calculate averages
    results = []
    for (actor1_id, actor1_name, actor2_id, actor2_name), data in pair_data.items():
        if data['count'] >= 3:
            avg_vote = sum(data['votes']) / len(data['votes']) if data['votes'] else 0
            results.append({
                'actor1': actor1_name,
                'actor2': actor2_name,
                'co_appearances': data['count'],
                'avg_vote_average': round(avg_vote, 2)
            })

    # Sort by co-appearances descending, then by avg_vote_average
    results.sort(key=lambda x: (-x['co_appearances'], -x['avg_vote_average']))

    # Print results
    print("\n" + "="*100)
    print("Query 2: Actor Pairs with ≥3 Co-Starring Movies")
    print("="*100)

    if results:
        print(f"\n{'Rank':<5} {'Actor 1':<30} {'Actor 2':<30} {'Co-Appearances':<15} {'Avg Vote':<10}")
        print("-"*100)
        for i, result in enumerate(results[:50], 1):  # Show top 50
            print(f"{i:<5} {result['actor1']:<30} {result['actor2']:<30} "
                  f"{result['co_appearances']:<15} {result['avg_vote_average']:<10.2f}")
    else:
        print("No results found.")

    print("="*100 + "\n")

    db_connector.close_connection()
    return results


if __name__ == '__main__':
    results = run_query()
import sys

sys.path.append('..')
from DbConnector import DbConnector


def run_query():
    db_connector = DbConnector(DATABASE='assignment3')
    db = db_connector.db

    pipeline = [
        {'$lookup': {
            'from': 'movies',
            'localField': 'id',
            'foreignField': 'id',
            'as': 'movie'
        }},

        {'$unwind': '$movie'},

        {'$match': {
            'movie.vote_count': {'$gte': 100}
        }},

        {'$unwind': '$crew'},
        {'$match': {'crew.job': 'Director'}},

        {'$addFields': {
            'director': '$crew'
        }},

        {'$unwind': '$cast'},

        # Group by director-actor pair
        {'$group': {
            '_id': {
                'director_id': '$director.id',
                'director_name': '$director.name',
                'actor_id': '$cast.id',
                'actor_name': '$cast.name'
            },
            'collaboration_count': {'$sum': 1},
            'avg_vote_average': {'$avg': '$movie.vote_average'},
            'avg_revenue': {'$avg': '$movie.revenue'}
        }},

        {'$match': {'collaboration_count': {'$gte': 3}}},

        {'$sort': {'avg_vote_average': -1}},

        {'$limit': 20},

        # Format output
        {'$project': {
            '_id': 0,
            'director': '$_id.director_name',
            'actor': '$_id.actor_name',
            'films_count': '$collaboration_count',
            'mean_vote_average': {'$round': ['$avg_vote_average', 2]},
            'mean_revenue': {'$round': ['$avg_revenue', 2]}
        }}
    ]

    results = list(db.credits.aggregate(pipeline))

    print("\n" + "="*120)
    print("Query 8: Top 20 Director-Actor Pairs (≥3 Collaborations, vote_count ≥100)")
    print("="*120)

    if results:
        print(f"\n{'Rank':<5} {'Director':<25} {'Actor':<25} {'Films':<7} {'Mean Vote':<11} {'Mean Revenue':<18}")
        print("-"*120)
        for i, result in enumerate(results, 1):
            print(f"{i:<5} {result['director'][:23]:<25} {result['actor'][:23]:<25} "
                  f"{result['films_count']:<7} {result['mean_vote_average']:<11.2f} "
                  f"${result['mean_revenue']:>15,.2f}")
    else:
        print("No results found.")

    print("="*120 + "\n")

    db_connector.close_connection()
    return results


if __name__ == '__main__':
    results = run_query()
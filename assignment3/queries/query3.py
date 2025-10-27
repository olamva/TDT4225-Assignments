import sys

sys.path.append('..')
from DbConnector import DbConnector


def run_query():
    db_connector = DbConnector(DATABASE='assignment3')
    db = db_connector.db

    pipeline = [
        {'$unwind': '$cast'},

        {'$lookup': {
            'from': 'movies',
            'localField': 'id',
            'foreignField': 'id',
            'as': 'movie'
        }},

        {'$unwind': '$movie'},

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

        {'$match': {'movie_count': {'$gte': 10}}},

        {'$addFields': {
            'genre_count': {'$size': '$genres'},
            'example_genres': {'$slice': ['$genres', 5]}
        }},

        {'$sort': {'genre_count': -1, 'movie_count': -1}},

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
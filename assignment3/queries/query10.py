import sys

sys.path.append('..')
from DbConnector import DbConnector


def run_query():
    db_connector = DbConnector(DATABASE='assignment3')
    db = db_connector.db

    pipeline = [
        {'$lookup': {
            'from': 'movies',
            'localField': 'movieId',
            'foreignField': 'id',
            'as': 'movie'
        }},

        {'$unwind': '$movie'},

        {'$unwind': {
            'path': '$movie.genres_list',
            'preserveNullAndEmptyArrays': True
        }},

        {'$group': {
            '_id': '$userId',
            'ratings_count': {'$sum': 1},
            'ratings': {'$push': '$rating'},
            'genres': {'$addToSet': '$movie.genres_list'}
        }},

        {'$match': {'ratings_count': {'$gte': 20}}},

        {'$addFields': {
            'mean_rating': {'$avg': '$ratings'},
            'genre_count': {'$size': '$genres'}
        }},

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

    genre_diverse = sorted(all_users, key=lambda x: x['genre_count'], reverse=True)[:10]

    high_variance = sorted(all_users, key=lambda x: x['variance'], reverse=True)[:10]

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
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
            'movie.release_date': {'$ne': None, '$exists': True}
        }},

        {'$unwind': '$cast'},

        {'$match': {
            'cast.order': {'$lte': 4},
            'cast.gender': {'$in': [1, 2]}
        }},

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

        {'$addFields': {
            'female_proportion': {
                '$divide': ['$female_count', '$total_count']
            }
        }},

        {'$group': {
            '_id': '$_id.decade',
            'movie_count': {'$sum': 1},
            'avg_female_proportion': {'$avg': '$female_proportion'}
        }},

        {'$sort': {'_id': 1}},

        # Format output
        {'$project': {
            '_id': 0,
            'decade': '$_id',
            'movie_count': 1,
            'avg_female_proportion': {'$round': ['$avg_female_proportion', 4]}
        }}
    ]

    results = list(db.credits.aggregate(pipeline))

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
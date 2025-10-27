import sys

sys.path.append('..')
from DbConnector import DbConnector


def run_query():
    db_connector = DbConnector(DATABASE='assignment3')
    db = db_connector.db

    pipeline = [
        {'$match': {
            'belongs_to_collection': {'$ne': None},
            'belongs_to_collection.name': {'$exists': True}
        }},

        {'$group': {
            '_id': '$belongs_to_collection.name',
            'movie_count': {'$sum': 1},
            'total_revenue': {'$sum': '$revenue'},
            'vote_averages': {'$push': '$vote_average'},
            'release_dates': {'$push': '$release_date'}
        }},

        {'$match': {'movie_count': {'$gte': 3}}},

        {'$addFields': {
            'sorted_votes': {'$sortArray': {'input': '$vote_averages', 'sortBy': 1}},
            'sorted_dates': {'$sortArray': {'input': '$release_dates', 'sortBy': 1}}
        }},

        {'$addFields': {
            'median_vote_average': {
                '$let': {
                    'vars': {
                        'size': {'$size': '$sorted_votes'},
                        'mid': {'$floor': {'$divide': [{'$size': '$sorted_votes'}, 2]}}
                    },
                    'in': {
                        '$cond': [
                            {'$eq': [{'$mod': ['$$size', 2]}, 0]},
                            {'$avg': [
                                {'$arrayElemAt': ['$sorted_votes', '$$mid']},
                                {'$arrayElemAt': ['$sorted_votes', {'$subtract': ['$$mid', 1]}]}
                            ]},
                            {'$arrayElemAt': ['$sorted_votes', '$$mid']}
                        ]
                    }
                }
            },
            'earliest_date': {'$arrayElemAt': ['$sorted_dates', 0]},
            'latest_date': {'$arrayElemAt': ['$sorted_dates', -1]}
        }},

        {'$sort': {'total_revenue': -1}},

        {'$limit': 10},

        # Format output
        {'$project': {
            '_id': 0,
            'collection': '$_id',
            'movie_count': 1,
            'total_revenue': 1,
            'median_vote_average': {'$round': ['$median_vote_average', 2]},
            'date_range': {
                '$concat': [
                    {'$ifNull': ['$earliest_date', 'N/A']},
                    ' → ',
                    {'$ifNull': ['$latest_date', 'N/A']}
                ]
            }
        }}
    ]

    results = list(db.movies.aggregate(pipeline))

    print("\n" + "="*120)
    print("Query 4: Top 10 Film Collections (≥3 movies) with Largest Total Revenue")
    print("="*120)

    if results:
        print(f"\n{'Rank':<5} {'Collection':<35} {'Movies':<8} {'Total Revenue':<18} {'Med. Vote':<11} {'Date Range':<30}")
        print("-"*120)
        for i, result in enumerate(results, 1):
            print(f"{i:<5} {result['collection']:<35} {result['movie_count']:<8} "
                  f"${result['total_revenue']:>15,.0f} {result['median_vote_average']:>11.2f} {result['date_range']:<30}")
    else:
        print("No results found.")

    print("="*120 + "\n")

    db_connector.close_connection()
    return results


if __name__ == '__main__':
    results = run_query()
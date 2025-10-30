import sys

sys.path.append('..')
from DbConnector import DbConnector


def run_query():
    db_connector = DbConnector(DATABASE='assignment3')
    db = db_connector.db
    pipeline = [
        {'$match': {
            'keywords': {'$elemMatch': {'$regex': 'noir', '$options': 'i'}},
            'vote_count': {'$gte': 50}
        }},

        {'$addFields': {
            'year': {
                '$cond': {
                    'if': {'$and': [
                        {'$ne': ['$release_date', None]},
                        {'$gte': [{'$strLenCP': '$release_date'}, 4]}
                    ]},
                    'then': {'$substr': ['$release_date', 0, 4]},
                    'else': 'N/A'
                }
            }
        }},

        {'$sort': {'vote_average': -1}},

        {'$limit': 20},

        # Format output
        {'$project': {
            '_id': 0,
            'title': 1,
            'year': 1,
            'vote_average': {'$round': ['$vote_average', 2]},
            'vote_count': 1,
        }}
    ]

    results = list(db.movies.aggregate(pipeline))

    print("\n" + "="*100)
    print("Query 7: Top 20 'Noir' Movies (vote_count ≥ 50) by Vote Average — matched on keywords")
    print("="*100)

    if results:
        print(f"\n{'Rank':<5} {'Title':<50} {'Year':<6} {'Vote Avg':<10} {'Vote Count':<12}")
        print("-"*100)
        for i, result in enumerate(results, 1):
            print(f"{i:<5} {result['title'][:48]:<50} {result['year']:<6} "
                  f"{result['vote_average']:<10.2f} {result['vote_count']:<12}")
    else:
        print("No results found.")

    print("="*100 + "\n")

    db_connector.close_connection()
    return results


if __name__ == '__main__':
    results = run_query()

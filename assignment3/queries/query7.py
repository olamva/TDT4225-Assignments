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

        {'$sort': {'vote_average': -1}},

        {'$limit': 20},

        # Format output
        {'$project': {
            '_id': 0,
            'title': 1,
            'vote_average': {'$round': ['$vote_average', 2]},
        }}
    ]

    results = list(db.movies.aggregate(pipeline))

    print("\n" + "="*100)
    print("Query 7: Top 20 'Noir' Movies (vote_count ≥ 50) by Vote Average — matched on keywords")
    print("="*100)

    if results:
        print(f"\n{'Rank':<5} {'Title':<50} {'Vote Avg':<10}")
        print("-"*100)
        for i, result in enumerate(results, 1):
            print(f"{i:<5} {result['title'][:48]:<50}"
                  f"{result['vote_average']:<10.2f}")
    else:
        print("No results found.")

    print("="*100 + "\n")

    db_connector.close_connection()
    return results


if __name__ == '__main__':
    results = run_query()

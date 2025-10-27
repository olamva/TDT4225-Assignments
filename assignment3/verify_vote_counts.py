"""
Verify vote counts in movies_metadata.csv against actual ratings in ratings.csv
"""

import numpy as np
import pandas as pd


def verify_vote_counts():
    """
    Compare vote_count in movies_metadata.csv with actual counts from ratings.csv
    """
    print("Loading data...")

    # Load original movies metadata
    movies_df = pd.read_csv('data/movies/movies_metadata.csv', low_memory=False)
    print(f"Loaded {len(movies_df)} movies")

    # Load original ratings
    ratings_df = pd.read_csv('data/movies/ratings.csv')
    print(f"Loaded {len(ratings_df)} ratings")

    # Load links to map movieId to tmdbId
    links_df = pd.read_csv('data/movies/links.csv')
    print(f"Loaded {len(links_df)} links\n")

    print("=" * 60)
    print("Calculating actual vote counts from ratings...")
    print("=" * 60)

    # Count ratings per movieId
    actual_vote_counts = ratings_df.groupby('movieId').size().to_dict()
    print(f"Calculated vote counts for {len(actual_vote_counts)} unique movieIds")

    # Convert links DataFrame types for mapping
    links_df['tmdbId'] = pd.to_numeric(links_df['tmdbId'], errors='coerce')
    links_df['movieId'] = pd.to_numeric(links_df['movieId'], errors='coerce')
    links_df = links_df.dropna(subset=['tmdbId', 'movieId'])
    links_df['tmdbId'] = links_df['tmdbId'].astype(int)
    links_df['movieId'] = links_df['movieId'].astype(int)

    # Map tmdbId -> vote counts (sum if multiple movieIds map to same tmdbId)
    vote_counts_by_tmdbid = {}
    for _, row in links_df.iterrows():
        tmdb_id = row['tmdbId']
        movie_id = row['movieId']

        if movie_id in actual_vote_counts:
            if tmdb_id not in vote_counts_by_tmdbid:
                vote_counts_by_tmdbid[tmdb_id] = 0
            vote_counts_by_tmdbid[tmdb_id] += actual_vote_counts[movie_id]

    print(f"Mapped vote counts for {len(vote_counts_by_tmdbid)} unique tmdbIds\n")

    print("=" * 60)
    print("Verifying vote counts in movies_metadata.csv...")
    print("=" * 60)

    # Filter to movies with valid IDs
    movies_df['id'] = pd.to_numeric(movies_df['id'], errors='coerce')
    movies_df = movies_df.dropna(subset=['id'])
    movies_df['id'] = movies_df['id'].astype(int)

    # Convert vote_count to numeric
    movies_df['vote_count'] = pd.to_numeric(movies_df['vote_count'], errors='coerce')

    # Compare vote counts
    stats = {
        'total_movies': 0,
        'movies_in_ratings': 0,
        'movies_not_in_ratings': 0,
        'correct_vote_count': 0,
        'incorrect_vote_count': 0,
        'missing_vote_count': 0,
        'differences': []
    }

    for idx, row in movies_df.iterrows():
        tmdb_id = row['id']
        csv_vote_count = row['vote_count']

        stats['total_movies'] += 1

        if tmdb_id in vote_counts_by_tmdbid:
            stats['movies_in_ratings'] += 1
            actual_count = vote_counts_by_tmdbid[tmdb_id]

            if pd.isna(csv_vote_count):
                stats['missing_vote_count'] += 1
                stats['differences'].append({
                    'id': tmdb_id,
                    'csv': 'NaN',
                    'actual': actual_count,
                    'diff': actual_count
                })
            elif int(csv_vote_count) == actual_count:
                stats['correct_vote_count'] += 1
            else:
                stats['incorrect_vote_count'] += 1
                diff = actual_count - int(csv_vote_count)
                stats['differences'].append({
                    'id': tmdb_id,
                    'csv': int(csv_vote_count),
                    'actual': actual_count,
                    'diff': diff
                })
        else:
            stats['movies_not_in_ratings'] += 1

    # Print summary
    print(f"\n{'=' * 60}")
    print("SUMMARY STATISTICS")
    print("=" * 60)
    print(f"Total movies in metadata: {stats['total_movies']:,}")
    print(f"Movies found in ratings:  {stats['movies_in_ratings']:,}")
    print(f"Movies NOT in ratings:    {stats['movies_not_in_ratings']:,}")
    print()
    print(f"Correct vote_count:       {stats['correct_vote_count']:,} ({stats['correct_vote_count']/stats['movies_in_ratings']*100:.1f}%)")
    print(f"Incorrect vote_count:     {stats['incorrect_vote_count']:,} ({stats['incorrect_vote_count']/stats['movies_in_ratings']*100:.1f}%)")
    print(f"Missing vote_count (NaN): {stats['missing_vote_count']:,} ({stats['missing_vote_count']/stats['movies_in_ratings']*100:.1f}%)")

    # Show some examples of differences
    if len(stats['differences']) > 0:
        print(f"\n{'=' * 60}")
        print("EXAMPLES OF INCORRECT VOTE COUNTS (first 20)")
        print("=" * 60)
        print(f"{'ID':<10} {'CSV':<10} {'Actual':<10} {'Difference'}")
        print("-" * 60)

        # Sort by absolute difference
        sorted_diffs = sorted(stats['differences'], key=lambda x: abs(x['diff']), reverse=True)

        for item in sorted_diffs[:20]:
            print(f"{item['id']:<10} {str(item['csv']):<10} {item['actual']:<10} {item['diff']:+}")

    # Additional stats
    if stats['differences']:
        diffs = [d['diff'] for d in stats['differences']]
        print(f"\n{'=' * 60}")
        print("DIFFERENCE STATISTICS")
        print("=" * 60)
        print(f"Average difference:       {np.mean(diffs):.2f}")
        print(f"Median difference:        {np.median(diffs):.2f}")
        print(f"Max overcount (CSV > actual): {min(diffs)}")
        print(f"Max undercount (CSV < actual): {max(diffs)}")
        print(f"Standard deviation:       {np.std(diffs):.2f}")

if __name__ == '__main__':
    verify_vote_counts()

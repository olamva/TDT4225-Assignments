import ast
from collections import Counter
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from tabulate import tabulate

data_dir = Path('../data/movies')
figures_dir = Path('../figures')
figures_dir.mkdir(exist_ok=True)

def analyze_movies_metadata(df):
    """Analyze movies_metadata.csv"""
    print("=== Movies Metadata Analysis ===")

    dtypes_table = [[col, str(dtype)] for col, dtype in df.dtypes.items()]
    print("Features and types:")
    print(tabulate(dtypes_table, headers=['Feature', 'Type'], tablefmt='grid'))

    print("\nMissing values and zero values:")
    missing = df.isnull().sum()
    for col in df.columns:
        null_count = missing[col]
        zero_count = (df[col] == 0).sum() if df[col].dtype in ['int64', 'float64'] else 0
        if null_count > 0 or zero_count > 0:
            print(f"  {col}: {null_count} null ({null_count/len(df)*100:.2f}%), {zero_count} zero ({zero_count/len(df)*100:.2f}%)")
    if missing.sum() == 0 and all((df[col] != 0).sum() == len(df) for col in df.columns if df[col].dtype in ['int64', 'float64']):
        print("  No missing or zero values found.")

    analyze_movies_metadata_specific(df)

def analyze_movies_metadata_specific(df):
    df['budget'] = pd.to_numeric(df['budget'], errors='coerce')
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')

    print("\n=== Movies Metadata Specific Analysis ===")

    print("Budget statistics:")
    print(f"  Mean: {df['budget'].mean():.2f}")
    print(f"  Median: {df['budget'].median():.2f}")
    print(f"  Max: {df['budget'].max():.2f}")
    print(f"  Min: {df['budget'].min():.2f}")

    print("Revenue statistics:")
    print(f"  Mean: {df['revenue'].mean():.2f}")
    print(f"  Median: {df['revenue'].median():.2f}")
    print(f"  Max: {df['revenue'].max():.2f}")
    print(f"  Min: {df['revenue'].min():.2f}")

    print("Runtime statistics:")
    print(f"  Mean: {df['runtime'].mean():.2f} minutes")
    print(f"  Median: {df['runtime'].median():.2f} minutes")
    print(f"  Max: {df['runtime'].max():.2f} minutes")
    print(f"  Min: {df['runtime'].min():.2f} minutes")
    movies_above_300 = df[df['runtime'] > 300].shape[0]
    print(f"  Movies with runtime above 300 minutes: {movies_above_300}")
    movies_zero_runtime = df[df['runtime'] == 0].shape[0]
    print(f"  Movies with 0 runtime: {movies_zero_runtime}")
    if movies_zero_runtime > 0:
        print("  Sample movies with 0 runtime:")
        zero_runtime_movies = df[df['runtime'] == 0][['title', 'release_date', 'genres']].head(10)
        for idx, row in zero_runtime_movies.iterrows():
            print(f"    {row['title']} ({row['release_date']}) - {row['genres']}")

    print(f"Average vote_average: {df['vote_average'].mean():.2f}")
    print(f"Average vote_count: {df['vote_count'].mean():.2f}")

    # Vote Average Distribution
    plt.figure(figsize=(10, 6))
    plt.hist(df['vote_average'].dropna(), bins=20, alpha=0.7)
    plt.title('Distribution of Vote Average')
    plt.xlabel('Vote Average')
    plt.ylabel('Frequency')
    plt.tight_layout()
    plt.savefig(figures_dir / 'vote_average_distribution.png')
    plt.close()

    # Vote Count Distribution
    plt.figure(figsize=(10, 6))
    plt.hist(df['vote_count'].dropna(), bins=20, alpha=0.7)
    plt.title('Distribution of Vote Count')
    plt.xlabel('Vote Count')
    plt.ylabel('Frequency')
    plt.tight_layout()
    plt.savefig(figures_dir / 'vote_count_distribution.png')
    plt.close()

    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
    plt.figure(figsize=(10, 6))
    df['release_date'].dt.year.dropna().hist(bins=50, alpha=0.7)
    plt.title('Distribution of Release Years')
    plt.xlabel('Year')
    plt.ylabel('Frequency')
    plt.savefig(figures_dir / 'release_date_hist.png')
    plt.close()

    plt.figure(figsize=(10, 6))
    df['runtime'].dropna().hist(bins=50, alpha=0.7)
    plt.title('Distribution of Runtime')
    plt.xlabel('Runtime (minutes)')
    plt.ylabel('Frequency')
    plt.savefig(figures_dir / 'runtime_hist.png')
    plt.close()

    percentile_99 = df['runtime'].quantile(0.99)
    plt.figure(figsize=(10, 6))
    runtime_capped = df['runtime'].dropna()
    runtime_capped = runtime_capped[runtime_capped <= percentile_99]
    runtime_capped.hist(bins=50, alpha=0.7)
    plt.title(f'Distribution of Runtime (capped at 99th percentile: {percentile_99:.1f} min)')
    plt.xlabel('Runtime (minutes)')
    plt.ylabel('Frequency')
    plt.savefig(figures_dir / 'runtime_hist_99percentile.png')
    plt.close()

    def extract_genres(genre_str):
        try:
            genres = ast.literal_eval(genre_str)
            return [g['name'] for g in genres] if isinstance(genres, list) else []
        except:
            return []

    df['genres_list'] = df['genres'].apply(extract_genres)
    df['main_genre'] = df['genres_list'].apply(lambda x: x[0] if x else 'Unknown')

    movies_without_genres = df['genres_list'].apply(lambda x: len(x) == 0).sum()
    print(f"Movies without genres: {movies_without_genres}")

    print("\n=== Genre Analysis ===")

    all_genres = []
    for genres_list in df['genres_list']:
        all_genres.extend(genres_list)
    unique_genres = sorted(set(all_genres))

    invalid_genres = {'Aniplex', 'BROSTA TV', 'Carousel Productions', 'GoHands',
                     'Mardock Scramble Production Committee', 'Odyssey Media',
                     'Pulser Productions', 'Rogue State', 'Sentai Filmworks',
                     'Telescene Film Group Productions', 'The Cartel',
                     'Vision View Entertainment'}
    valid_genres = [g for g in unique_genres if g not in invalid_genres]

    print(f"Number of different genres: {len(valid_genres)}")
    print(f"All genres: {', '.join(valid_genres)}")
    print(f"\nInvalid entries found (production companies): {len(invalid_genres)}")
    print(f"  {', '.join(sorted(invalid_genres))}")

    df_exploded = df.explode('genres_list')
    df_exploded = df_exploded[df_exploded['genres_list'].notna() & (df_exploded['genres_list'] != '')]
    df_exploded = df_exploded[~df_exploded['genres_list'].isin(invalid_genres)]

    print("\n=== Average and Median Runtime for Each Genre (All 20 Genres) ===")
    genre_runtime_stats = df_exploded.groupby('genres_list')['runtime'].agg(['mean', 'median', 'count']).sort_values('mean', ascending=False)
    genre_runtime_stats.columns = ['Avg Runtime', 'Median Runtime', 'Movie Count']
    print(tabulate(genre_runtime_stats, headers='keys', tablefmt='grid', floatfmt='.2f'))

    print("\n=== Genres Ranked by Average Vote (All 20 Genres) ===")
    genre_vote_stats = df_exploded.groupby('genres_list')['vote_average'].agg(['mean', 'median', 'count']).sort_values('mean', ascending=False)
    genre_vote_stats.columns = ['Avg Vote', 'Median Vote', 'Movie Count']
    print(tabulate(genre_vote_stats, headers='keys', tablefmt='grid', floatfmt='.2f'))

    # Analyze the 12 invalid "genres" (production companies)
    print("\n=== Invalid Genre Entries (Production Companies) ===")
    df_invalid = df.explode('genres_list')
    df_invalid = df_invalid[df_invalid['genres_list'].notna() & (df_invalid['genres_list'] != '')]
    df_invalid = df_invalid[df_invalid['genres_list'].isin(invalid_genres)]
    
    if len(df_invalid) > 0:
        print(f"Found {len(df_invalid)} entries with invalid genres")
        print("\nRuntime and Vote Statistics for Invalid Genres:")
        invalid_stats = df_invalid.groupby('genres_list').agg({
            'runtime': ['mean', 'median', 'count'],
            'vote_average': ['mean', 'median']
        })
        invalid_stats.columns = ['Avg Runtime', 'Median Runtime', 'Movie Count', 'Avg Vote', 'Median Vote']
        print(tabulate(invalid_stats.sort_index(), headers='keys', tablefmt='grid', floatfmt='.2f'))
        
        print("\nSample movies with invalid genre entries:")
        for invalid_genre in sorted(invalid_genres):
            genre_movies = df_invalid[df_invalid['genres_list'] == invalid_genre]
            if len(genre_movies) > 0:
                sample = genre_movies.head(1)
                for idx, row in sample.iterrows():
                    print(f"  {invalid_genre}: ID={row['id']}, Title={row['title']}, Date={row['release_date']}")
    else:
        print("No movies found with invalid genre entries (expected - these are corrupted rows)")

    unique_statuses = df['status'].dropna().unique()
    print(f"Unique status values ({len(unique_statuses)}): {list(unique_statuses)}")

    status_counts = df['status'].value_counts()
    print("\nDistribution of movies by status:")
    for status, count in status_counts.items():
        print(f"  {status}: {count} movies ({count/len(df)*100:.2f}%)")

    plt.figure(figsize=(12, 8))
    genre_runtime = df.groupby('main_genre')['runtime'].mean().sort_values()
    plt.scatter(range(len(genre_runtime)), genre_runtime.values, alpha=0.7)
    plt.xticks(range(len(genre_runtime)), genre_runtime.index, rotation=45, ha='right')
    plt.title('Average Runtime by Genre')
    plt.xlabel('Genre')
    plt.ylabel('Average Runtime (minutes)')
    plt.tight_layout()
    plt.savefig(figures_dir / 'genre_runtime_scatter.png')
    plt.close()

    plt.figure(figsize=(12, 8))
    genre_budget = df.groupby('main_genre')['budget'].mean().sort_values()
    plt.scatter(range(len(genre_budget)), genre_budget.values, alpha=0.7)
    plt.xticks(range(len(genre_budget)), genre_budget.index, rotation=45, ha='right')
    plt.title('Average Budget by Genre')
    plt.xlabel('Genre')
    plt.ylabel('Average Budget')
    plt.tight_layout()
    plt.savefig(figures_dir / 'genre_budget_scatter.png')
    plt.close()

    plt.figure(figsize=(10, 6))
    valid_data = df.dropna(subset=['budget', 'revenue'])
    valid_data = valid_data[(valid_data['budget'] > 0) & (valid_data['revenue'] > 0)]
    plt.scatter(valid_data['budget'], valid_data['revenue'], alpha=0.5)
    plt.title('Budget vs Revenue')
    plt.xlabel('Budget')
    plt.ylabel('Revenue')
    plt.savefig(figures_dir / 'budget_revenue_scatter.png')
    plt.close()

    plt.figure(figsize=(10, 6))
    df['popularity'] = pd.to_numeric(df['popularity'], errors='coerce')
    valid_data = df.dropna(subset=['runtime', 'popularity'])
    valid_data = valid_data[(valid_data['runtime'] > 0) & (valid_data['popularity'] > 0)]
    plt.scatter(valid_data['runtime'], valid_data['popularity'], alpha=0.5)
    plt.title('Runtime vs Popularity')
    plt.xlabel('Runtime (minutes)')
    plt.ylabel('Popularity')
    plt.savefig(figures_dir / 'runtime_popularity_scatter.png')
    plt.close()
    print("Runtime vs Popularity scatter plot saved to figures/runtime_popularity_scatter.png")

    def extract_names(field_str, key='name'):
        try:
            items = ast.literal_eval(field_str)
            if isinstance(items, list):
                return [item[key] for item in items if key in item]
            elif isinstance(items, dict) and key in items:
                return [items[key]]
            else:
                return []
        except:
            return []

    all_prod_companies = []
    for comp_str in df['production_companies'].dropna():
        all_prod_companies.extend(extract_names(comp_str))
    unique_prod_companies = set(all_prod_companies)
    print(f"Number of unique production companies: {len(unique_prod_companies)}")
    company_counts = Counter(all_prod_companies)
    top_companies = company_counts.most_common(3)
    print("Top 3 production companies:")
    for company, count in top_companies:
        print(f"  {company}: {count}")

    all_prod_countries = []
    for country_str in df['production_countries'].dropna():
        all_prod_countries.extend(extract_names(country_str))
    unique_prod_countries = set(all_prod_countries)
    print(f"Number of unique production countries: {len(unique_prod_countries)}")
    country_counts = Counter(all_prod_countries)
    top_countries = country_counts.most_common(10)
    print("Top 3 production countries:")
    for country, count in top_countries[:3]:
        print(f"  {country}: {count}")

    countries, counts = zip(*top_countries)
    plt.figure(figsize=(12, 6))
    plt.bar(countries, counts, alpha=0.7)
    plt.title('Top 10 Production Countries by Number of Movies')
    plt.xlabel('Country')
    plt.ylabel('Number of Movies')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(figures_dir / 'production_countries_bar.png')
    plt.close()

    all_spoken_langs = []
    for lang_str in df['spoken_languages'].dropna():
        all_spoken_langs.extend(extract_names(lang_str))
    unique_spoken_langs = set(all_spoken_langs)
    print(f"Number of unique spoken languages: {len(unique_spoken_langs)}")
    lang_counts = Counter(all_spoken_langs)
    top_langs = lang_counts.most_common(3)
    print("Top 3 spoken languages:")
    for lang, count in top_langs:
        print(f"  {lang}: {count}")
    avg_movies_per_lang = len(all_spoken_langs) / len(unique_spoken_langs) if unique_spoken_langs else 0
    print(f"Average number of movies per spoken language: {avg_movies_per_lang:.2f}")

    all_collections = []
    for coll_str in df['belongs_to_collection'].dropna():
        all_collections.extend(extract_names(coll_str))
    unique_collections = set(all_collections)
    print(f"Number of unique collections: {len(unique_collections)}")
    collection_counts = Counter(all_collections)
    top_collections = collection_counts.most_common(10)
    print("Top 10 collections:")
    for coll, count in top_collections:
        print(f"  {coll}: {count}")
    avg_movies_per_collection = len(all_collections) / len(unique_collections) if unique_collections else 0
    print(f"Average number of movies in a collection: {avg_movies_per_collection:.2f}")

def main():
    data_path = data_dir / 'movies_metadata.csv'
    if data_path.exists():
        df = pd.read_csv(data_path, low_memory=False)
        analyze_movies_metadata(df)
    else:
        print(f"Data file not found: {data_path}")

if __name__ == '__main__':
    main()
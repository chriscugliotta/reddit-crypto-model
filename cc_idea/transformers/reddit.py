import logging
import multiprocessing as mp
import pandas as pd
from datetime import datetime, timedelta
from pandas import DataFrame
from pathlib import Path
from textblob import TextBlob
from typing import List, Tuple
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from cc_idea.core.config import paths
from cc_idea.extractors.reddit import load_comments
from cc_idea.utils.pandas_utils import epoch_to_est
log = logging.getLogger(__name__)
sia = SentimentIntensityAnalyzer()



# TODO:  Fix bug:  Function `transform_comments` does not respect start/end date ranges.
# TODO:  Shrink dataframe early to reduce memory during parallel sentiment step.
def transform_comments(q: str, metas: List[dict]) -> DataFrame:

    # Log.
    log.debug(f'Begin with q = {q}, metas = {len(metas)}.')

    # To reduce memory, process each year separately.
    years = sorted({meta['date'].year for meta in metas})
    frames = [_transform_comments_in_year(q, metas, year) for year in years]
    df = pd.concat(frames, ignore_index=True)

    # Log, return.
    log.debug(f'Done with q = {q}, rows = {df.shape[0]}.')
    return df


def _transform_comments_in_year(q: str, metas: List[dict], year: int) -> DataFrame:

    # Get maximum date in inbound data.
    max_date_inbound = max(x['date'] for x in metas if x['date'].year == year)

    # Get maximum date in cache file (if it exists).
    max_date_cached, cache_path = _check_cache(q, year)

    # How many inbound days need to be processed?
    log.debug(f'q = {q}, year = {year}, max_date_cached = {max_date_cached:%m-%d}, max_date_inbound = {max_date_inbound:%m-%d}, not_cached_days = {(max_date_inbound - max_date_cached).days}.')

    # Stop early if all inbound data has already been processed.
    if max_date_cached >= max_date_inbound:
        return pd.read_parquet(cache_path)

    # Get inbound records that need to be processed.
    df_comments = load_comments(
        q=q,
        start_date=max_date_cached + timedelta(days=1),
        end_date=max_date_inbound,
    )

    # Perform sentiment analysis.
    df_sentiments = _analyze_comments(df_comments)

    # Join, aggregate, validate, cache.
    return (df_comments
        .pipe(_join_sentiments, df_sentiments)
        .pipe(_aggregate_comments, q, year)
        .pipe(_update_cache, q, year, cache_path, max_date_inbound)
    )


def _check_cache(q: str, year: int) -> Tuple[datetime, Path]:
    """
    Returns the maximum date and file path for a given cache (if it exists).

    Note:
        Each (q, year) combination is cached separately into a parquet file named like:
        `q={q}/year={year}/max_date={YYYY-MM-DD}.snappy.parquet`, where `max_date` indicates the
        maximum date within the parquet file.  This `max_date` tag is used to guide our incremental
        cache refresh logic.
    """

    cache_dir = paths.data / 'reddit_comments_transformed' / f'q={q}' / f'year={year}'
    cache_paths = list(cache_dir.glob('*.snappy.parquet'))

    if len(cache_paths) > 0:
        cache_path = cache_paths[0]
        max_date_cached = datetime.strptime(cache_path.name.split('.')[0].split('=')[1], '%Y-%m-%d')
    else:
        cache_path = None
        max_date_cached = datetime(year, 1, 1, 0, 0, 0) - timedelta(days=1)

    return max_date_cached, cache_path


def _analyze_comments(df_comments: DataFrame) -> DataFrame:
    """Performs sentiment analysis on one year's worth of comments."""

    # Prepare comments as a list of (index, body) tuples.
    inputs = (df_comments
        .loc[:, ['body']]
        .to_records()
    )

    # Start timer.
    start_time = datetime.now()

    # Sentiment analysis is computationally expensive.
    # For big data, it's faster to distribute and parallelize the work across multiple processes.
    # For small data, it's faster to simply use a single process (due to overhead of spawning processes).
    if len(inputs) < 5000:
        log.debug(f'Analyzing {len(inputs):,} comments using 1 process.')
        outputs = [_analyze_comment(x) for x in inputs]

    else:
        processes = int(mp.cpu_count())
        chunk_size = int(len(inputs) / processes) + 1
        log.debug(f'Analyzing {len(inputs):,} comments using {processes} procesess.')
        with mp.Pool(processes=processes) as pool:
            outputs = pool.map(_analyze_comment, inputs, chunksize=chunk_size)

    # Stop timer.
    end_time = datetime.now()
    elapsed_time = (end_time - start_time).total_seconds()
    log.debug(f'Analyzed {len(inputs):,} comments in {elapsed_time:.2f} seconds ({1000 * elapsed_time / len(inputs):.2f} ms per comment).')

    # Return outputted tuples as dataframe.
    return (pd
        .DataFrame(outputs, columns=['index', 'negative', 'neutral', 'positive', 'composite', 'polarity', 'subjectivity'])
        .set_index('index', drop=True)
    )


def _analyze_comment(row: tuple) -> tuple:
    """Performs sentiment analysis (both Vader and TextBlob) on given text string."""
    index = row[0]
    text = row[1]
    vader = sia.polarity_scores(text)
    blob = TextBlob(text)
    return (index, vader['neg'], vader['neu'], vader['pos'], vader['compound'], blob.sentiment.polarity, blob.sentiment.subjectivity)


def _join_sentiments(df_comments: DataFrame, df_sentiments: DataFrame) -> DataFrame:
    """Joins sentiments onto original comments dataframe."""

    df = (df_comments
        .assign(**{
            'created_date': lambda df: epoch_to_est(df['created_utc']).dt.floor('D'),
        })
        .loc[:, [
            'id',
            'created_utc',
            'created_date',
            'author',
            'subreddit',
            'body',
            'score',
        ]]
        .join(df_sentiments)
    )

    assert df.shape[0] == df_comments.shape[0], 'Row count changed.'
    assert df_comments.shape[0] == df_comments['id'].drop_duplicates().shape[0], 'Unique key violated.'
    assert df['negative'].isnull().sum() == 0, 'Not-null constraint violated.'
    return df


def _aggregate_comments(df_comments: DataFrame, q: str, year: int) -> DataFrame:
    """Aggregates comments, scores, and sentiments up to (q, created_date) level."""

    columns = [
        'negative',
        'neutral',
        'positive',
        'composite',
        'polarity',
        'subjectivity',
    ]

    def get_weighted(df):
        for column in columns:
            df['w_' + column] = df['score'] * df[column]
        return df

    def get_averages(df):
        for column in ['score'] + columns:
            df['avg_' + column] = df[column] / df['num_comments']
        for column in columns:
            df['avg_w_' + column] = df['w_' + column] / df['num_comments']
        return df

    df = (df_comments
        .assign(q=q)
        .assign(num_comments=1)
        .pipe(get_weighted)
        .loc[:, ['q', 'created_date', 'num_comments', 'score'] + columns + ['w_' + x for x in columns]]
        .groupby(['q', 'created_date'], as_index=False)
        .sum()
        .pipe(get_averages)
    )

    assert df['created_date'].dt.year.drop_duplicates().tolist() == [year]
    log.debug(f'Aggregated {df_comments.shape[0]:,} comments into {df.shape[0]:,} rows.')
    return df


def _update_cache(df_agg: DataFrame, q: str, year: int, old_cache_path: Path, max_date_inbound: datetime) -> DataFrame:
    """Caches the result of all (slow) transformations performed above."""

    # If a cache already exists, we will append to it.
    if old_cache_path is not None and old_cache_path.is_file():
        df_old = pd.read_parquet(old_cache_path)
        df_agg = pd.concat([df_old, df_agg], ignore_index=True)

    # Create new cache.
    cache_prefix = paths.data / 'reddit_comments_transformed'
    cache_path = paths.data / 'reddit_comments_transformed' / f'q={q}' / f'year={year}' / f'max_date={max_date_inbound:%Y-%m-%d}.snappy.parquet'
    cache_path.parent.mkdir(exist_ok=True, parents=True)
    df_agg.to_parquet(cache_path, index=False)
    log.debug(f'Cached {df_agg.shape[0]:,} rows at:  {cache_path.relative_to(cache_prefix).as_posix()}.')

    # Delete old cache.
    if old_cache_path is not None and old_cache_path.is_file():
        old_cache_path.unlink()
        log.debug(f'Deleted {df_old.shape[0]:,} rows at:  {old_cache_path.relative_to(cache_prefix).as_posix()}.')

    return df_agg

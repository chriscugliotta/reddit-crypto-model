import logging
import multiprocessing as mp
import pandas as pd
from datetime import date, datetime, timedelta
from pandas import DataFrame
from pathlib import Path
from textblob import TextBlob
from typing import List, Tuple
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from cc_idea.core.config import paths
from cc_idea.extractors.reddit import load_reddit
from cc_idea.utils.date_utils import epoch_to_est
log = logging.getLogger(__name__)
sia = SentimentIntensityAnalyzer()



# TODO:  Finalize aggregated metrics.
# TODO:  Handle submissions.
def transform_reddit(endpoint: str, q: str, metas: List[dict], chunk_size: int = 100) -> DataFrame:

    # Log.
    log.debug(f'Begin with endpoint = {endpoint}, q = {q}, metas = {len(metas)}.')

    # Get already-cached date range (if any cache exists).
    min_date_cached, max_date_cached, cache_path = _check_cache(endpoint, q)

    # How many inbound days need to be processed?
    inbound = [
        meta
        for meta in metas
        if (min_date_cached is None or meta['date'] < min_date_cached)
        or (max_date_cached is None or meta['date'] > max_date_cached)
    ]
    log.debug(f'min_date_cached = {min_date_cached}, max_date_cached = {max_date_cached}, not_cached_days = {len(inbound)}.')

    # Stop early if all inbound data has already been processed.
    if len(inbound) == 0:
        return pd.read_parquet(cache_path)

    # To reduce memory, process N megabytes at a time.
    chunk = []
    size = 0
    for i, meta in enumerate(inbound):
        chunk += [meta]
        size += meta['path'].stat().st_size
        if size > (chunk_size * 1e6) or i == len(inbound) - 1:
            df = _transform_chunk(endpoint, q, chunk, size)
            chunk = []
            size = 0

    # Log, return.
    log.debug(f'Done with endpoint = {endpoint}, q = {q}, rows = {df.shape[0]}.')
    return df


def _check_cache(endpoint: str, q: str) -> Tuple[date, date, Path]:
    """
    Returns the date range and file path for a given cache (if it exists).

    Note:
        Each `q` value is transformed and cached separately into a parquet file named like:
        `q={q}/min_date={min_date}, max_date={max_date}.snappy.parquet`, where `min_date` and
        `max_date` indicate the date range within the parquet file.  This file naming pattern is
        used to optimize our incremental cache refresh logic.
    """

    cache_dir = paths.data / f'reddit_{endpoint}s_aggregated' / f'q={q}'
    cache_paths = list(cache_dir.glob('*.snappy.parquet'))

    if len(cache_paths) > 0:
        cache_path = cache_paths[0]
        min_date = datetime.strptime(cache_path.name[9:19], '%Y-%m-%d').date()
        max_date = datetime.strptime(cache_path.name[30:40], '%Y-%m-%d').date()
    else:
        cache_path = None
        min_date = None
        max_date = None

    return min_date, max_date, cache_path


def _transform_chunk(endpoint: str, q: str, metas: List[dict], size: int) -> DataFrame:

    # Log
    log.debug(f'Begin with endpoint = {endpoint}, q = {q}, metas = {len(metas):,}, size = {size / 1e6:.2f} MB.')

    # To reduce memory, we will only load a subset of columns.
    columns = [
        'id',
        'created_utc',
        'author',
        'subreddit',
        'body',
        'score',
    ]

    # Get inbound records that need to be processed.
    df_comments = load_reddit(
        endpoint=endpoint,
        search=('q', q),
        start_date=None,
        end_date=None,
        metas=metas,
        columns=columns,
    )

    # Perform sentiment analysis.
    df_sentiments = _analyze_comments(df_comments)

    # Join, aggregate, validate, cache.
    return (df_comments
        .pipe(_join_sentiments, df_sentiments)
        .pipe(_aggregate_comments, endpoint, q)
        .pipe(_update_cache, endpoint, q)
    )


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


def _aggregate_comments(df_comments: DataFrame, endpoint: str, q: str) -> DataFrame:
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

    log.debug(f'Aggregated {df_comments.shape[0]:,} comments into {df.shape[0]:,} rows.')
    return df


def _update_cache(df_agg: DataFrame, endpoint: str, q: str) -> DataFrame:
    """Caches the result of all (slow) transformations performed above."""

    # Does a previous cache already exist?
    old_min_date, old_max_date, old_cache_path = _check_cache(endpoint, q)

    # If so, we will append to it.
    if old_cache_path is not None:
        df_old = pd.read_parquet(old_cache_path)
        df_agg = pd.concat([df_old, df_agg], ignore_index=True)

    # Get new date range.
    min_date = df_agg['created_date'].min().date()
    max_date = df_agg['created_date'].max().date()

    # Create new cache.
    cache_prefix = paths.data / f'reddit_{endpoint}s_aggregated'
    cache_path = cache_prefix / f'q={q}' / f'min_date={min_date}, max_date={max_date}.snappy.parquet'
    cache_path.parent.mkdir(exist_ok=True, parents=True)
    df_agg.to_parquet(cache_path, index=False)
    log.debug(f'Cached {df_agg.shape[0]:,} rows at:  {cache_path.relative_to(cache_prefix).as_posix()}.')

    # Delete old cache.
    if old_cache_path is not None:
        old_cache_path.unlink()
        log.debug(f'Deleted {df_old.shape[0]:,} rows at:  {old_cache_path.relative_to(cache_prefix).as_posix()}.')

    return df_agg

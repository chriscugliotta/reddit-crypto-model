import logging
import multiprocessing as mp
import pandas
from datetime import datetime
from pathlib import Path
from pandas import DataFrame
from textblob import TextBlob
from typing import Dict, List, Tuple
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from rcm.core.cache import DateCache, DateRangeCache
from rcm.core.config import paths, config
from rcm.extractors.reddit import RedditExtractor
from rcm.utils.date_utils import epoch_to_est
log = logging.getLogger(__name__)
sia = SentimentIntensityAnalyzer()



class SentimentTransformer:

    def __init__(self):
        self.schema: Dict[str, str] = None
        self.unique_key: List[str] = None

    def transform(self, endpoint: str, search: Tuple[str, str], min_score: int, caches: List[DateCache], chunk_size: int = None) -> DateRangeCache:
        """
        Performs sentiment analysis on Reddit comments or submissions.

        This function takes the Pushshift API responses, i.e. the zipped JSONs collected in a
        previous step by RedditExtractor, and passes them through two sentiment analysis libraries:
        Vader Sentiment and TextBlob.  Sentiment calculations can be slow and resource-intensive,
        thus multiple performance optimizations have been implemented:

            1.  To reduce runtime, this code uses a multiprocessing pool to divide the sentiment
                calculations across N worker processes.

            2.  To reduce memory, this code divides the input data into chunks, and each chunk is
                calculated sequentially.  (Each single chunk is evenly split across the workers.)

        When finished, the result is cached as a parquet file.  This parquet contains a curated
        subset of columns from the original API response, plus some additional columns for the
        sentiment values produced by Vader and TextBlob.  Thus, this transformation step is actually
        doing two things:  calculating sentiments, and also converting the deeply-nested API
        response JSONs into a simpler, _flattened_, tabular structure.  Perhaps one day, this
        tabular structure could be stored in a SQL database.

        Args:
            endpoint (str):
                Pushshift API endpoint.  Either `comment` or `submission`.

            search (Tuple[str, str]):
                Word or subreddit filter, e.g. `(word, doge)` or `(subreddit, dogelore)`.

            min_score (int):
                Minimum score filter.
                All comments (or submissions) with score < `min_score` will be omitted.

            caches (List[DateCache]):
                Pushshift API responses cached by RedditExtractor.

            chunk_size (int):
                Maximum size (in megabytes) of each chunk.

        Returns:
            DateRangeCache:  Parquet file containing Reddit data and corresponding sentiment values.
        """

        # Log.
        log.debug(f'Begin with endpoint = {endpoint}, {search[0]} = {search[1]}, caches = {len(caches)}.')

        # Get already-cached date range (if any cache exists).
        range_cache = DateRangeCache.from_prefix(self._get_cache_prefix(endpoint, search, min_score))

        # How many inbound days need to be processed?
        inbound = [
            cache
            for cache in caches
            if (range_cache.min_date is None or cache.date < range_cache.min_date)
            or (range_cache.max_date is None or cache.date > range_cache.max_date)
        ]
        log.debug(f'min_date_cached = {range_cache.min_date}, max_date_cached = {range_cache.max_date}, not_cached_days = {len(inbound)}.')

        # Stop early if all inbound data has already been processed.
        if len(inbound) == 0:
            return range_cache

        # To reduce memory, process N megabytes at a time.
        frames = []
        chunk = []
        size = 0
        chunk_size = chunk_size if chunk_size else config.transformers.sentiment.chunk_size
        for i, cache in enumerate(inbound):
            chunk += [cache]
            size += cache.path.stat().st_size
            if size > (chunk_size * 1e6) or i == len(inbound) - 1:
                df = self._transform_chunk(endpoint, search, min_score, chunk, size)
                frames += [df]
                chunk = []
                size = 0

        # Update cache.
        df = pandas.concat(frames, ignore_index=True)
        range_cache.append(df, 'created_date', min(x.date for x in caches), max(x.date for x in caches))

        # Log, return.
        log.debug(f'Done with endpoint = {endpoint}, {search[0]} = {search[1]}, rows = {len(df):,}.')
        return range_cache

    def _transform_chunk(self, endpoint: str, search: Tuple[str, str], min_score: int, caches: List[dict], size: int) -> DataFrame:

        # Log
        log.debug(f'Begin with endpoint = {endpoint}, {search[0]} = {search[1]}, caches = {len(caches):,}, size = {size / 1e6:.2f} MB.')

        # Get inbound records that need to be processed.
        df_comments = RedditExtractor().read(
            endpoint=endpoint,
            search=search,
            min_score=min_score,
            min_date=None,
            max_date=None,
            caches=caches,
        )

        # Which text column should we analyze?
        if endpoint == 'comment':
            column = 'body'
        if endpoint == 'submission':
            column = 'title'

        # Perform sentiment analysis.
        df_sentiments = self._analyze_comments(df_comments, column)

        # Join.
        return (df_comments
            .pipe(self._join_sentiments, df_sentiments)
        )

    def _analyze_comments(self, df_comments: DataFrame, column: str) -> DataFrame:
        """Performs sentiment analysis on one 'chunk' of comments."""

        # Prepare comments as a list of (index, sentence) tuples.
        inputs = (df_comments
            .loc[:, [column]]
            .to_records()
        )

        # Start timer.
        start_time = datetime.now()
        processes = config.transformers.sentiment.processes

        # Sentiment analysis is computationally expensive.
        # For big data, it's faster to distribute and parallelize the work across multiple processes.
        # For small data, it's faster to simply use a single process (due to overhead of spawning processes).
        if len(inputs) < 5000 or processes == 1:
            log.debug(f'Analyzing {len(inputs):,} comments using 1 process.')
            outputs = [self._analyze_comment(x) for x in inputs]

        else:
            chunk_size = int(len(inputs) / processes) + 1
            log.debug(f'Analyzing {len(inputs):,} comments using {processes} processes.')
            with mp.Pool(processes=processes) as pool:
                outputs = pool.map(self._analyze_comment, inputs, chunksize=chunk_size)

        # Stop timer.
        end_time = datetime.now()
        elapsed_time = (end_time - start_time).total_seconds()
        average_time = 1000 * elapsed_time / len(inputs) if len(inputs) != 0 else 0
        log.debug(f'Analyzed {len(inputs):,} comments in {elapsed_time:.2f} seconds ({average_time:.2f} ms per comment).')

        # Return outputted tuples as dataframe.
        return (
            DataFrame(outputs, columns=['index', 'negative', 'neutral', 'positive', 'compound', 'polarity', 'subjectivity'])
            .set_index('index', drop=True)
        )

    def _analyze_comment(self, row: tuple) -> tuple:
        """Performs sentiment analysis (both Vader and TextBlob) on given text string."""
        index = row[0]
        text = row[1]
        vader = sia.polarity_scores(text)
        blob = TextBlob(text)
        return (index, vader['neg'], vader['neu'], vader['pos'], vader['compound'], blob.sentiment.polarity, blob.sentiment.subjectivity)

    def _join_sentiments(self, df_comments: DataFrame, df_sentiments: DataFrame) -> DataFrame:
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
                'title',
                'body',
                'score',
            ]]
            .join(df_sentiments)
        )

        # TODO:  Move validation logic into a base class.
        assert len(df) == len(df_comments), 'Row count changed.'
        assert df_comments.shape[0] == df_comments['id'].drop_duplicates().shape[0], 'Unique key violated.'
        assert df['negative'].isnull().sum() == 0, 'Not-null constraint violated.'
        return df

    def _get_cache_prefix(self, endpoint: str, search: Tuple[str, str], min_score: int) -> Path:
        """Returns cache path prefix for given endpoint and search filter."""
        return (
            paths.data /
            f'reddit_{endpoint}s_sentiment' /
            f'min_score={min_score}' /
            f'{search[0]}={search[1]}'
        )

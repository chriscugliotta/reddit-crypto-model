import logging
import multiprocessing as mp
import pandas
from datetime import datetime
from pathlib import Path
from pandas import DataFrame
from textblob import TextBlob
from typing import List
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from cc_idea.core.cache import DateCache, DateRangeCache
from cc_idea.core.config import paths
from cc_idea.extractors.reddit import RedditExtractor
from cc_idea.utils.date_utils import epoch_to_est
log = logging.getLogger(__name__)
sia = SentimentIntensityAnalyzer()



class SentimentTransformer:

    def __init__(self):
        self.schema = 'TBD'
        self.unique_key = 'TBD'

    def transform(self, endpoint: str, filters: dict, caches: List[DateCache], chunk_size: int = 100) -> DateRangeCache:
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

            filters (Dict[str, str]):
                Search filters.
                Examples:  `{word: doge}` or `{min_score: 2, subreddit: dogelore}`.

            caches (List[DateCache]):
                Cached Pushshift API responses returned by RedditExtractor.

            chunk_size (int):
                Maximum size (in megabytes) of each chunk.

        Returns:
            DateRangeCache:  Parquet file containing Reddit data and corresponding sentiment values.
        """

        # Log.
        log.debug(f'Begin with endpoint = {endpoint}, filters = {filters}, caches = {len(caches)}.')

        # Get already-cached date range (if any cache exists).
        range_cache = DateRangeCache.from_prefix(self._get_cache_prefix(endpoint, filters))

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
        for i, cache in enumerate(inbound):
            chunk += [cache]
            size += cache.path.stat().st_size
            if size > (chunk_size * 1e6) or i == len(inbound) - 1:
                df = self._transform_chunk(endpoint, filters, chunk, size, range_cache)
                frames += [df]
                chunk = []
                size = 0

        # Update cache.
        df = pandas.concat(frames, ignore_index=True)
        range_cache.append(df, 'created_date', min(x.date for x in caches), max(x.date for x in caches))

        # Log, return.
        log.debug(f'Done with endpoint = {endpoint}, filters = {filters}, rows = {len(df):,}.')
        return range_cache

    def _transform_chunk(self, endpoint: str, filters: dict, caches: List[dict], size: int, range_cache: DateRangeCache) -> DataFrame:

        # Log
        log.debug(f'Begin with endpoint = {endpoint}, filters = {filters}, caches = {len(caches):,}, size = {size / 1e6:.2f} MB.')

        # Get inbound records that need to be processed.
        df_comments = RedditExtractor().read(
            endpoint=endpoint,
            filters=filters,
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

        # Sentiment analysis is computationally expensive.
        # For big data, it's faster to distribute and parallelize the work across multiple processes.
        # For small data, it's faster to simply use a single process (due to overhead of spawning processes).
        if len(inputs) < 5000:
            log.debug(f'Analyzing {len(inputs):,} comments using 1 process.')
            outputs = [self._analyze_comment(x) for x in inputs]

        else:
            processes = int(mp.cpu_count())
            chunk_size = int(len(inputs) / processes) + 1
            log.debug(f'Analyzing {len(inputs):,} comments using {processes} processes.')
            with mp.Pool(processes=processes) as pool:
                outputs = pool.map(self._analyze_comment, inputs, chunksize=chunk_size)

        # Stop timer.
        end_time = datetime.now()
        elapsed_time = (end_time - start_time).total_seconds()
        log.debug(f'Analyzed {len(inputs):,} comments in {elapsed_time:.2f} seconds ({1000 * elapsed_time / len(inputs):.2f} ms per comment).')

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

    def _get_cache_prefix(self, endpoint: str, filters: dict) -> Path:
        """Returns cache path prefix for given endpoint and filter set."""
        min_score = filters.get('min_score') if filters.get('min_score') else 'null'
        suffix = ', '.join(f'{k}={v}' for k, v in filters.items() if k != 'min_score')
        return paths.data / f'reddit_{endpoint}s_sentiment' / f'min_score={min_score}' / suffix

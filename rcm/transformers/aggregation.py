import logging
import pandas
from pathlib import Path
from pandas import DataFrame
from typing import Dict, List, Tuple
from rcm.core.cache import DateRangeCache
from rcm.core.config import paths, config
from rcm.core.transformer import Transformer
from rcm.utils.pandas_utils import _insert
log = logging.getLogger(__name__)



# TODO:  Update cache incrementally.
class AggregationTransformer(Transformer):

    def __init__(self):
        self.schema: Dict[str, str] = {
            'endpoint': 'string',
            'search': 'string',
            'created_date': 'datetime64',
            'num_samples': 'int',
            'num_rockets': 'int',
            'num_positive': 'int',
            'num_negative': 'int',
            'sum_score': 'int',
            'wnum_rockets': 'int',
            'wnum_positive': 'int',
            'wnum_negative': 'int',
            'wsum_positive': 'float',
            'wsum_negative': 'float',
            'wsum_compound': 'float',
            'wsum_polarity': 'float',
            'wavg_subjectivity': 'float',
            'wavg_positive': 'float',
            'wavg_negative': 'float',
            'wavg_compound': 'float',
            'wavg_polarity': 'float',
            'wavg_subjectivity': 'float',
        }
        self.unique_key: List[str] = [
            'endpoint',
            'search',
            'created_date',
        ]

    def _transform(self, data: Dict) -> DataFrame:
        """Aggregates Reddit comments and submissions up to (endpoint, search, date) level."""

        # Log
        log.info('Begin.')

        # Get already-cached date range (if any cache exists).
        cache = DateRangeCache.from_prefix(self._get_cache_prefix())
        min_date = config.extractors.reddit.min_date
        max_date = config.extractors.reddit.max_date

        # Stop early if all inbound data has already been processed.
        if cache.max_date is not None and cache.max_date >= max_date:
            log.info(f'Cache is up-to-date.')
            return cache.load()

        # Aggregate.
        frames = []
        for endpoint in ['comment', 'submission']:
            for search, sentiments in data[f'reddit_{endpoint}s_sentiment'].items():
                df = self._transform_chunk(endpoint, search, sentiments.load())
                frames += [df]

        # Cache.
        df = pandas.concat(frames, ignore_index=True)
        cache.overwrite(df, 'created_date', min_date, max_date)

        # Log, return.
        log.info(f'Done with rows = {len(df):,}.')
        return df

    def _transform_chunk(self, endpoint: str, search: Tuple[str, str], df: DataFrame) -> DataFrame:

        # Aggregate.
        df_agg = (
            df
            .assign(**{
                'num_samples': 1,
                'num_rockets': lambda x: x['body' if endpoint == 'comment' else 'title'].str.count('🚀'),
                'num_positive': lambda x: (x['positive'] > x['negative']) | (x['polarity'] > 0),
                'num_negative': lambda x: (x['negative'] > x['positive']) | (x['polarity'] < 0),
                'sum_score': lambda x: x['score'],
                'wnum_rockets': lambda x: x['score'] * x['num_rockets'],
                'wnum_positive': lambda x: x['score'] * x['num_positive'],
                'wnum_negative': lambda x: x['score'] * x['num_negative'],
                'wsum_positive': lambda x: x['score'] * x['positive'],
                'wsum_negative': lambda x: x['score'] * x['negative'],
                'wsum_compound': lambda x: x['score'] * x['compound'],
                'wsum_polarity': lambda x: x['score'] * x['polarity'],
                'wsum_subjectivity': lambda x: x['score'] * x['subjectivity'],
            })
            .groupby(['created_date'])
            .agg({
                'num_samples': 'sum',
                'num_rockets': 'sum',
                'num_positive': 'sum',
                'num_negative': 'sum',
                'sum_score': 'sum',
                'wnum_rockets': 'sum',
                'wnum_positive': 'sum',
                'wnum_negative': 'sum',
                'wsum_positive': 'sum',
                'wsum_negative': 'sum',
                'wsum_compound': 'sum',
                'wsum_polarity': 'sum',
                'wsum_subjectivity': 'sum',
            })
            .assign(**{
                'wavg_positive': lambda x: x['wsum_positive'] / x['sum_score'],
                'wavg_negative': lambda x: x['wsum_negative'] / x['sum_score'],
                'wavg_compound': lambda x: x['wsum_compound'] / x['sum_score'],
                'wavg_polarity': lambda x: x['wsum_polarity'] / x['sum_score'],
                'wavg_subjectivity': lambda x: x['wsum_subjectivity'] / x['sum_score'],
            })
            .sort_values(by='created_date')
            .reset_index()
            .pipe(_insert, 0, 'endpoint', endpoint)
            .pipe(_insert, 1, 'search', f'{search[0]}={search[1]}')
        )

        # Log, return.
        log.debug(f'Done with endpoint = {endpoint}, {search[0]} = {search[1]}, df = {len(df):,}, df_agg = {len(df_agg):,}.')
        return df_agg

    def _get_cache_prefix(self) -> Path:
        return paths.data / 'reddit_aggregations'

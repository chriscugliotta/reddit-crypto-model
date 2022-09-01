import logging
import pandas
from pandas import DataFrame
from typing import Dict, List
from rcm.core.config import config
from rcm.core.transformer import Transformer
log = logging.getLogger(__name__)



class DensifyTransformer(Transformer):

    def __init__(self):
        self.schema: Dict[str, str] = None
        self.unique_key: List[str] = [
            'symbol_id',
            'date',
        ]
        self.not_null: List[str] = [
            'symbol_id',
            'date',
        ]

    def _transform(self, data: Dict):
        """Constructs a 'dense' feature matrix compatible with SKLearn."""
        log.info('Begin.')
        data['dt_s2ys'] = self._get_df_symbol_to_yahoo_symbol()
        data['dt_s2rq'] = self._get_df_symbol_to_reddit_query()
        data['dt_reddit_staging'] = self._get_df_reddit_staging(data)
        data['dt_available_dates'] = self._get_df_available_dates(data)
        data['dt_calendar'] = self._get_df_calendar(data)
        data['dt_features'] = self._get_df_features(data)
        log.info(f'Done with row count = {len(data["dt_features"]):,}.')
        return data['dt_features']

    def _get_df_symbol_to_yahoo_symbol(self) -> DataFrame:
        """Returns symbol-to-Yahoo-symbol mapping."""
        return DataFrame([
            {'symbol_id': symbol_id, 'yahoo_symbol': symbol.yahoo_symbol}
            for symbol_id, symbol in config.symbols.items()
        ])

    def _get_df_symbol_to_reddit_query(self) -> DataFrame:
        """Returns symbol-to-Reddit-query mapping."""
        rows = []
        for symbol_id, symbol in config.symbols.items():
            for endpoint in ['comment', 'submission']:
                for word in symbol.words:
                    rows += [(symbol_id, endpoint, f'word={word}')]
                for subreddit in symbol.subreddits:
                    rows += [(symbol_id, endpoint, f'subreddit={subreddit}')]
        return DataFrame(rows, columns=['symbol_id', 'endpoint', 'search'])

    def _get_df_reddit_staging(self, data: Dict) -> DataFrame:
        """Aggregates Reddit data over `search` dimension up to `(symbol, endpoint, date)` level."""
        return (
            data['reddit_aggregations']
            .merge(data['dt_s2rq'], how='left', on=['endpoint', 'search'])
            .groupby(['symbol_id', 'endpoint', 'created_date'], as_index=False)
            .sum()
            .pipe(self._update_wavg)
        )

    def _update_wavg(self, df: DataFrame) -> DataFrame:
        """Re-calculates weighted-average metrics.  (Needed after aggregation.)"""
        return df.assign(**{
            'wavg_positive': lambda x: x['wsum_positive'] / x['sum_score'],
            'wavg_negative': lambda x: x['wsum_negative'] / x['sum_score'],
            'wavg_compound': lambda x: x['wsum_compound'] / x['sum_score'],
            'wavg_polarity': lambda x: x['wsum_polarity'] / x['sum_score'],
            'wavg_subjectivity': lambda x: x['wsum_subjectivity'] / x['sum_score'],
        })

    def _get_df_available_dates(self, data: Dict) -> DataFrame:
        """For each symbol, get effective min/max dates across all data sources."""
        return ( pandas
            .merge(
                left=self._get_df_yahoo_dates(data),
                right=self._get_df_reddit_dates(data),
                how='left',
                on='symbol_id',
            )
            .assign(**{
                'min_available_date': lambda x: x[['min_yahoo_date', 'min_reddit_date']].max(axis=1),
                'max_available_date': lambda x: x[['max_yahoo_date', 'max_reddit_date']].min(axis=1),
                'available_days': lambda x: (x['max_available_date'] - x['min_available_date']).dt.days + 1,
            })
        )

    def _get_df_yahoo_dates(self, data: Dict) -> DataFrame:
        """For each symbol, get min/max dates in Yahoo data."""
        return (
            data['yahoo_finance_price_history']
            .assign(**{
                'min_yahoo_date': lambda x: x['date'],
                'max_yahoo_date': lambda x: x['date'],
            })
            .groupby('symbol', as_index=False)
            .agg({
                'min_yahoo_date': 'min',
                'max_yahoo_date': 'max',
            })
            .rename(columns={'symbol': 'yahoo_symbol'})
            .merge(data['dt_s2ys'], how='left', on='yahoo_symbol')
            .loc[:, ['symbol_id', 'yahoo_symbol', 'min_yahoo_date', 'max_yahoo_date']]
        )

    def _get_df_reddit_dates(self, data: Dict) -> DataFrame:
        """"For each symbol, get min/max dates in Reddit data."""
        return (
            data['dt_s2ys']
            .loc[:, ['symbol_id']]
            .assign(**{
                'min_reddit_date': pandas.to_datetime(config.extractors.reddit.min_date),
                'max_reddit_date': pandas.to_datetime(config.extractors.reddit.max_date),
            })
        )

    def _get_df_calendar(self, data: Dict) -> DataFrame:
        """Returns 'dense' date range for each symbol.  (Missing dates are injected.)"""
        frames = []
        for row in data['dt_available_dates'].itertuples():
            df = DataFrame(pandas.date_range(row.min_available_date, row.max_available_date), columns=['date'])
            df.insert(0, 'symbol_id', row.symbol_id)
            df.insert(1, 'yahoo_symbol', row.yahoo_symbol)
            frames += [df]
        return pandas.concat(frames, ignore_index=True)

    def _get_df_features(self, data: Dict) -> DataFrame:
        """Joins Yahoo and Reddit features onto 'dense' calendar records."""
        return (
            data['dt_calendar']
            .merge(
                right=self._get_df_yahoo_features(data, 'p'),
                how='left',
                on=['yahoo_symbol', 'date'],
            )
            .merge(
                right=self._get_df_yahoo_features(data, 'pa', aggregate=True),
                how='left',
                on=['date'],
            )
            .merge(
                right=self._get_df_reddit_features(data, 'comment', 'rc'),
                how='left',
                on=['symbol_id', 'date'],
            )
            .merge(
                right=self._get_df_reddit_features(data, 'submission', 'rs'),
                how='left',
                on=['symbol_id', 'date'],
            )
            .merge(
                right=self._get_df_reddit_features(data, 'comment', 'rca', aggregate=True),
                how='left',
                on=['date'],
            )
            .merge(
                right=self._get_df_reddit_features(data, 'submission', 'rsa', aggregate=True),
                how='left',
                on=['date'],
            )
            .drop(columns=['yahoo_symbol'])
        )

    def _get_df_yahoo_features(self, data: Dict, prefix: str, aggregate: bool = False) -> DataFrame:
        """TODO:  Explain."""
        renames = {
            column: prefix + '_' + column
            for column in data['yahoo_finance_price_history'].columns
            if column not in ['symbol', 'date']
        }
        def _aggregate(df):
            return (
                df
                .groupby('date', as_index=False)
                .sum()
            )
        return (
            data['yahoo_finance_price_history']
            .pipe(_aggregate if aggregate else (lambda x: x))
            .rename(columns=renames)
            .rename(columns={'symbol': 'yahoo_symbol'})
        )

    def _get_df_reddit_features(self, data: Dict, endpoint: str, prefix: str, aggregate: bool = False) -> DataFrame:
        """TODO:  Explain."""
        renames = {
            column: prefix + '_' + column
            for column in data['dt_reddit_staging'].columns
            if column not in ['symbol_id', 'endpoint', 'created_date']
        }
        def _aggregate(df):
            return (
                df
                .groupby('created_date', as_index=False)
                .sum()
                .pipe(self._update_wavg)
            )
        return (
            data['dt_reddit_staging']
            .loc[lambda x: x['endpoint'] == endpoint]
            .drop(columns=['endpoint'])
            .pipe(_aggregate if aggregate else (lambda x: x))
            .rename(columns=renames)
            .rename(columns={'created_date': 'date'})
        )

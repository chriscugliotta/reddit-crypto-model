import logging
import pandas
import yfinance as yf
from datetime import date, timedelta
from pandas import DataFrame
from typing import Dict, List
from cc_idea.core.cache import DateRangeCache
from cc_idea.core.config import paths
from cc_idea.core.extractor import Extractor
log = logging.getLogger(__name__)



class YahooFinanceExtractor(Extractor):

    def __init__(self):
        self.schema: Dict[str, str] = {
            'symbol': 'string',
            'date': 'datetime64',
            'open': 'float64',
            'high': 'float64',
            'low': 'float64',
            'close': 'float64',
            'volume': 'int64',
            'dividends': 'int64',
            'stock_splits': 'int64',
        }
        self.unique_key: List[str] = [
            'symbol',
            'date',
        ]

    def extract(self, symbols: List[str], read: bool = False) -> List[DateRangeCache]:
        """
        Extracts (and caches) the daily price history for given list of symbols.

        Args:
            symbols (List[str]):
                List of stock symbols recognized by Yahoo Finance, e.g. `[VTI, BTC-USD]`.

            read (bool):
                If true, dataframe is returned instead of cache objects.

        Returns:
            List[DateRangeCache]:  List of cache objects.
        """
        caches = [self._extract_symbol(x) for x in symbols]
        return caches if not read else self.read(symbols)

    def _extract_symbol(self, symbol: str) -> DateRangeCache:
        """Extracts (and caches) the daily price history for a single symbol."""

        # Get cache object.
        prefix = paths.data / 'yahoo_finance_price_history' / f'symbol={symbol}'
        cache = DateRangeCache.from_prefix(prefix)

        # If cache is empty or stale, hit the API, and cache the result.
        # Never load current date (to prevent stale snapshot in cache).
        if cache.max_date is None or cache.max_date < date.today() - timedelta(days=1):
            ticker = yf.Ticker(symbol)
            df = ticker.history(period='max')
            df.insert(0, 'symbol', symbol)
            df = df.reset_index()
            df = df[df['Date'] < date.today().strftime('%Y-%m-%d')]
            df = cache.overwrite(df, 'Date')
            del df

        return cache

    def _read(self, symbols: List[str] = None, caches: List[DateRangeCache] = None) -> DataFrame:
        """Reads previously-cached data into a dataframe."""

        # If cache targets not provided, search for them.
        if caches is None:
            caches = [
                DateRangeCache.from_prefix(x.parent)
                for x in (paths.data / 'yahoo_finance_price_history').rglob('*.snappy.parquet')
                if symbols is None or x.parts[-2][7:] in symbols
            ]

        # Read cache objects into dataframe.
        # Also, rename columns from PascalCase to snake_case.
        df = pandas.concat([cache.load() for cache in caches], ignore_index=True)
        renames = {column: column.lower().replace(' ', '_') for column in df}
        df = df.rename(columns=renames)

        # Log, return.
        log.debug(f'Done:  records = {len(df):,}, symbols = {len(caches):,}.')
        return df

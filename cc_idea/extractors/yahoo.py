import logging
import pandas as pd
import yfinance as yf
from datetime import date, timedelta
from pandas import DataFrame
from cc_idea.core.cache import DateRangeCache
from cc_idea.core.config import paths
log = logging.getLogger(__name__)



def load_prices(symbol: str) -> DataFrame:
    """Returns complete price history (at daily granularity) for given symbol."""

    # Get cache object.
    prefix = paths.data / 'yahoo_finance_price_history' / f'symbol={symbol}'
    cache = DateRangeCache.from_prefix(prefix)

    # If cache is empty or stale, hit the API, and cache the result.
    # Never load current date (to prevent stale snapshot in cache).
    if cache.max_date is None or cache.max_date < date.today() - timedelta(days=1):
        ticker = yf.Ticker(symbol)
        df = ticker.history(period='max')
        df = df.reset_index()
        df = df[df['Date'] < date.today().strftime('%Y-%m-%d')]
        df = cache.overwrite(df, 'Date')
        del df

    # Get result from cache.
    df = cache.load()
    log.debug(f'Fetched {len(df):,} records for symbol = {symbol}.')

    # Validate and rename columns.
    columns = {
        'date': {'rename': 'Date', 'type': 'datetime64[ns]'},
        'open': {'rename': 'Open', 'type': 'float64'},
        'high': {'rename': 'High', 'type': 'float64'},
        'low': {'rename': 'Low', 'type': 'float64'},
        'close':  {'rename': 'Close', 'type': 'float64'},
        'volume':  {'rename': 'Volume', 'type': 'int64'},
        'dividends':  {'rename': 'Dividends', 'type': 'int64'},
        'stock_splits':  {'rename': 'Stock Splits',  'type': 'int64'},
    }
    df = df.rename(columns={v['rename']: k for k, v in columns.items()})
    df = df.astype({k: v['type'] for k, v in columns.items()})
    df.insert(0, 'symbol', symbol)
    return df

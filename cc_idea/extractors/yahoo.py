import logging
import pandas as pd
import yfinance as yf
from pandas import DataFrame
from cc_idea.core.config import paths
log = logging.getLogger(__name__)



def load_prices(symbol: str) -> DataFrame:
    """Returns complete price history (at daily granularity) for given symbol."""

    # Get cache path for upcoming request.
    cache_path = paths.data / 'yahoo_finance_price_history' / f'symbol={symbol}' / '0.csv.gz'
    cache_path.parent.mkdir(exist_ok=True, parents=True)

    # Get price history via Yahoo Finance API.
    # TODO:  Figure out caching logic.
    if not cache_path.is_file():
        ticker = yf.Ticker(symbol)
        df = ticker.history(period='max')
        df.to_csv(cache_path, compression='gzip')
        del df

    # Get result from cache.
    df = pd.read_csv(cache_path)
    log.debug(f'Fetched {df.shape[0]:,} records for symbol = {symbol}.')

    # Validate and rename columns.
    columns = {
        'date': {'rename': 'Date', 'type': 'datetime64[ns]'},
        'open': {'rename': 'Open', 'type': 'float'},
        'high': {'rename': 'High', 'type': 'float'},
        'low': {'rename': 'Low', 'type': 'float'},
        'close':  {'rename': 'Close', 'type': 'float'},
        'volume':  {'rename': 'Volume', 'type': 'int'},
        'dividends':  {'rename': 'Dividends', 'type': 'int'},
        'stock_splits':  {'rename': 'Stock Splits',  'type': 'int'},
    }
    df = df.rename(columns={v['rename']: k for k, v in columns.items()})
    df = df.astype({k: v['type'] for k, v in columns.items()})
    df.insert(0, 'symbol', symbol)

    return df
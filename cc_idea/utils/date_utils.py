import pandas as pd
from datetime import date, datetime
from pandas import Series
from pathlib import Path


def date_to_datetime(x: date) -> datetime:
    """Converts a date to a datetime."""
    return datetime(x.year, x.month, x.day)


def epoch_to_est(column: Series):
    """Converts an epoch (e.g. 1580531187) to a timezone-naive EST timestamp (e.g. 2020-01-01 00:00:00)."""
    # Convert from epoch to UTC.
    column = pd.to_datetime(column, unit='s')
    # Convert from UTC to EST.
    column = column.dt.tz_localize('UTC').dt.tz_convert('America/New_York')
    # Convert from timezone-aware to timezone-naive.  (Yikes.)
    return pd.to_datetime(column.dt.strftime('%Y-%m-%d %H:%M:%S.%f'))


def path_to_date(path: Path) -> date:
    """Converts a date-partitioned file path into a date."""
    day = path.parts[-2][4:]
    month = path.parts[-3][6:]
    year = path.parts[-4][5:]
    return datetime.strptime(f'{year}-{month}-{day}', '%Y-%m-%d').date()

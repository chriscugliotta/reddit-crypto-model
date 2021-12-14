import pandas as pd
from pandas import Series



def epoch_to_est(column: Series):
    """Converts an epoch (e.g. 1580531187) to a timezone-naive EST timestamp (e.g. 2020-01-01 00:00:00)."""
    # Convert from epoch to UTC.
    column = pd.to_datetime(column, unit='s')
    # Convert from UTC to EST.
    column = column.dt.tz_localize('UTC').dt.tz_convert('EST')
    # Convert from timezone-aware to timezone-naive.  (Yikes.)
    return pd.to_datetime(column.dt.strftime('%Y-%m-%d %H:%M:%S.%f'))

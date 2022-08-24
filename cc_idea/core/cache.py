import gzip
import json
import logging
import pandas as pd
from datetime import datetime, date
from pandas import DataFrame
from pathlib import Path
log = logging.getLogger(__name__)



class DateCache:
    """
    A cache file containing a single date of timephased data.

    Note:
        This codebase aims to encapsulate all long-term storage via `Cache` classes.  This way, if
        we ever decide to swap local disk for S3, the `Cache` classes can be modified accordingly,
        and the rest of the codebase can be agnostic to the change.
    """

    def __init__(self, date: date, prefix: Path, suffix: str = '.json.gz'):
        self.date: date = date
        self.prefix: str = prefix
        self.suffix: str = suffix
        self.path: Path = prefix / f'year={date.strftime("%Y")}' / f'month={date.strftime("%m")}' / f'day={date.strftime("%d")}' / f'0{suffix}'

    def save(self, data: dict):
        """Saves data to cache."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(self.path, 'wt') as file:
            json.dump(data, file)

    def load(self) -> dict:
        """Reads data from cache."""
        with gzip.open(self.path, 'rt') as file:
            return json.load(file)



class DateRangeCache:
    """A cache file containing an arbitrary date range of timephased data."""

    @classmethod
    def from_prefix(cls, prefix: Path, suffix: str = '.snappy.parquet') -> 'DateRangeCache':
        """
        Finds the cache file at the given prefix, then instantiates an object that describes it.

        Note:
            Each `q` value is transformed and cached separately into a parquet file named like:
            `q={q}/min_date={min_date}, max_date={max_date}.snappy.parquet`, where `min_date` and
            `max_date` indicate the date range within the parquet file.  This file naming pattern is
            used to optimize our incremental cache refresh logic.
        """
        paths = list(prefix.glob('*' + suffix))
        if len(paths) > 1:
            raise Exception(f'Unexpected cache file count:  count = {len(paths)}, prefix = {prefix}.')
        if len(paths) == 1:
            min_date = datetime.strptime(paths[0].name[9:19], '%Y-%m-%d').date()
            max_date = datetime.strptime(paths[0].name[30:40], '%Y-%m-%d').date()
        else:
            min_date = None
            max_date = None
        return cls(min_date, max_date, prefix, suffix)

    def __init__(self, min_date: date, max_date: str, prefix: Path, suffix: str):
        self.min_date: date = min_date
        self.max_date: date = max_date
        self.prefix: Path = prefix
        self.suffix: str = suffix
        self.path: Path = prefix / f'min_date={min_date}, max_date={max_date}{suffix}'

    def save(self, data: DataFrame):
        """Saves data to cache."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data.to_parquet(self.path, index=False)

    def load(self) -> DataFrame:
        """Reads data from cache."""
        return pd.read_parquet(self.path)

    def append(self, new_data: DataFrame, date_column: str, min_date: date = None, max_date: date = None) -> DataFrame:
        """
        Appends inbound data to existing cache file.  If no file exists, a new one is created.

        Args:
            new_data (DataFrame):
                Inbound data.

            date_column (str):
                Dataframe column that `self.min_date` and `self.max_date` should be sourced from.

        Returns:
            DataFrame:  Union of existing data and inbound data.
        """

        # Does a previous cache already exist?  If so, we will append to it.
        if self.path.is_file():
            old_data = self.load()
            new_data = pd.concat([old_data, new_data], ignore_index=True)

        # Get new date range.
        old_path = self.path
        self.min_date = new_data[date_column].min().date() if min_date is None else min_date
        self.max_date = new_data[date_column].max().date() if max_date is None else max_date
        self.path = self.prefix / f'min_date={self.min_date}, max_date={self.max_date}{self.suffix}'

        # Create new cache file.
        self.save(new_data)
        log.debug(f'Cached {len(new_data):,} rows at:  {self.path.relative_to(self.prefix.parent).as_posix()}.')

        # Delete old cache file.
        if old_path.is_file():
            old_path.unlink()
            log.debug(f'Deleted {len(old_data):,} rows at:  {old_path.relative_to(self.prefix.parent).as_posix()}.')

        return new_data

    def overwrite(self, new_data: DataFrame, date_column: str, min_date: date = None, max_date: date = None) -> DataFrame:

        # Does a previous cache already exist?  If so, we will delete it.
        if self.path.is_file():
            self.path.unlink()

        # Get new date range.
        self.min_date = new_data[date_column].min().date() if min_date is None else min_date
        self.max_date = new_data[date_column].max().date() if max_date is None else max_date
        self.path = self.prefix / f'min_date={self.min_date}, max_date={self.max_date}{self.suffix}'

        # Create new cache file.
        self.save(new_data)
        log.debug(f'Cached {len(new_data):,} rows at:  {self.path.relative_to(self.prefix.parent).as_posix()}.')
        return new_data

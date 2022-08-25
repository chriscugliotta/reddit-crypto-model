from pandas import DataFrame
from typing import Dict, List



class Extractor:
    """
    Base extractor class.

    Note:
        All extractors should implement `extract` and `_read` methods.  The `extract` method should
        pull data from a (slow) remote source, and cache it to local disk.  The `_read` method
        should load previously-cached data into memory, and return it as a dataframe.
    """

    def __init__(self):
        self.schema: Dict[str, str] = None
        self.unique_key: List[str] = None

    def extract(self, *args, **kwargs):
        raise NotImplementedError

    def read(self, *args, **kwargs) -> DataFrame:
        df = self._read(*args, **kwargs)
        df = self._validate(df)
        return df

    def _read(self, *args, **kwargs) -> DataFrame:
        raise NotImplementedError

    def _validate(self, df: DataFrame) -> DataFrame:

        # If dataframe is empty, ensure all columns exist.
        if self.schema is not None and len(df) == 0:
            df = DataFrame([], columns=self.schema.keys())

        # Validate column existence and data types.
        if self.schema is not None:
            df = df[self.schema.keys()]
            df = df.astype(self.schema)

        # Validate unique key.
        if self.unique_key is not None:
            if len(df) != len(df[self.unique_key].drop_duplicates()):
                raise Exception('Unique key violated.')

        return df

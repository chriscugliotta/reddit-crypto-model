from pandas import DataFrame
from typing import Dict, List



class Transformer:
    """
    Base transformer class.

    Note:
        All transformers should implement the `_transform` method.
    """

    def __init__(self):
        self.schema: Dict[str, str] = None
        self.unique_key: List[str] = None
        self.not_null: List[str] = None

    def transform(self, *args, **kwargs) -> DataFrame:
        df = self._transform(*args, **kwargs)
        df = self._validate(df)
        return df

    def _transform(self, *args, **kwargs):
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

        # Validate not-null constraints.
        if self.not_null is not None:
            for column in self.not_null:
                if df[column].isnull().sum() > 0:
                    raise Exception(f'Not-null constraint violated:  {column}.')

        return df

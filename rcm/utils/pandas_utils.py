from pandas import DataFrame
from typing import Any



def _insert(df: DataFrame, index: int, column: str, value: Any) -> DataFrame:
    """Can be used to call Pandas `insert` function via `pipe` function (for method chaining)."""
    df.insert(index, column, value)
    return df

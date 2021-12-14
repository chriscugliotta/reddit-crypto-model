import logging
import pandas as pd
from pandas import DataFrame, Series
from cc_idea.core.config import paths
from cc_idea.utils.excel_utils import to_excel
from cc_idea.utils.pandas_utils import epoch_to_est
log = logging.getLogger(__name__)



def load(df_prices: DataFrame, df_comments: DataFrame) -> DataFrame:
    """This report shows historical relationship between comments and prices."""

    # Aggregate comments.
    df_comment_counts = (df_comments
        .copy()
        .assign(created_date=lambda df: epoch_to_est(df['created_utc']).dt.floor('D'))
        .assign(num_comments=1)
        .groupby('created_date', as_index=False)[['num_comments', 'score']]
        .sum()
    )

    # Get min/max available dates.
    min_price_date = df_prices['date'].min()
    max_price_date = df_prices['date'].max()
    min_comment_date = df_comment_counts['created_date'].min()
    max_comment_date = df_comment_counts['created_date'].max()
    min_date = max(min_price_date, min_comment_date)
    max_date = min(max_price_date, max_comment_date)

    # Construct report data set.
    df_report = (df_prices
        .loc[:, ['symbol', 'date', 'open', 'close']]
        .merge(
            right=df_comment_counts.rename(columns={'created_date': 'date'}),
            how='outer',
            on='date',
        )
        .sort_values(by='date')
        .loc[lambda df: df['date'] >= min_date]
        .loc[lambda df: df['date'] <= max_date]
    )

    # Load data into Excel template.
    to_excel(
        template_file=paths.reports / 'price_history' / 'template.xlsx',
        output_file=paths.repo / 'price_history_out.xlsx',
        sheet_name='Data',
        top_left=(2,1),
        df=df_report,
        fill_down_styles=True,
    )

    # Return dataframe.
    return df_report

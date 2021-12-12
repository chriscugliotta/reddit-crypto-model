from datetime import datetime
from cc_idea.extractors.reddit import get_comments
from cc_idea.extractors.yahoo import get_prices
from cc_idea.reports.price_history.report import load



def test_price_history_report():

    df_comments = get_comments(
        q='doge',
        start_date=datetime(2020, 1, 1, 0, 0, 0),
        end_date=datetime(2020, 2, 1, 0, 0, 0),
    )

    df_prices = get_prices(
        symbol='DOGE-USD',
    )

    df_report = load(df_prices, df_comments)
    df_report = df_report.set_index('date')

    assert round(df_report.loc['2020-01-01']['open'], 6) == 0.002028
    assert round(df_report.loc['2020-01-15']['open'], 6) == 0.002468
    assert round(df_report.loc['2020-01-31']['open'], 6) == 0.002440

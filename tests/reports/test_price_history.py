from datetime import date
from cc_idea.extractors.reddit import load_reddit
from cc_idea.extractors.yahoo import load_prices
from cc_idea.reports.price_history.report import load_report



def test_price_history_report():

    df_comments = load_reddit(
        endpoint='comment',
        min_date=date(2020, 1, 1),
        max_date=date(2020, 1, 8),
        filters={'min_score': 2, 'word': 'doge'},
    )

    df_prices = load_prices(
        symbol='DOGE-USD',
    )

    df_report = load_report(df_prices, df_comments)
    df_report = df_report.set_index('date')

    assert round(df_report.loc['2020-01-01']['open'], 6) == 0.002028
    assert round(df_report.loc['2020-01-02']['open'], 6) == 0.002034
    assert round(df_report.loc['2020-01-03']['open'], 6) == 0.002008

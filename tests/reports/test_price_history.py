from datetime import date
from rcm.extractors.reddit import RedditExtractor
from rcm.extractors.yahoo import YahooFinanceExtractor
from rcm.reports.price_history.report import load_report



def test_price_history_report():

    df_comments = RedditExtractor().extract(
        endpoint='comment',
        search=('word', 'doge'),
        min_score=3,
        min_date=date(2020, 1, 1),
        max_date=date(2020, 1, 8),
        read=True,
    )

    df_prices = YahooFinanceExtractor().extract(
        symbols=['DOGE-USD'],
        read=True,
    )

    df_report = load_report(df_prices, df_comments)
    df_report = df_report.set_index('date')

    assert round(df_report.loc['2020-01-01']['open'], 6) == 0.002028
    assert round(df_report.loc['2020-01-02']['open'], 6) == 0.002034
    assert round(df_report.loc['2020-01-03']['open'], 6) == 0.002008

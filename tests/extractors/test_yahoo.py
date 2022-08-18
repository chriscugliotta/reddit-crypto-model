import shutil
from cc_idea.core.config import paths
from cc_idea.extractors.yahoo import YahooFinanceExtractor


def test_yahoo_extractor():

    cache_path = paths.data / 'yahoo_finance_price_history' / 'symbol=MSFT'
    shutil.rmtree(cache_path, ignore_errors=True)

    df = YahooFinanceExtractor().extract(symbols=['MSFT'], read=True)
    df = df.set_index('date')

    assert round(df.loc['2018-12-31']['open'], 2) ==  97.31
    assert round(df.loc['2019-12-31']['open'], 2) == 152.84
    assert round(df.loc['2020-12-31']['open'], 2) == 218.43

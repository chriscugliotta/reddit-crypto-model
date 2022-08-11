import shutil
from cc_idea.core.config import paths
from cc_idea.extractors.yahoo import load_prices


def test_load_prices():

    cache_path = paths.data / 'yahoo_finance_price_history' / 'symbol=MSFT'
    shutil.rmtree(cache_path, ignore_errors=True)

    df = load_prices('MSFT')
    df = df.set_index('date')

    assert round(df.loc['2018-12-31']['open'], 2) ==  97.52
    assert round(df.loc['2019-12-31']['open'], 2) == 153.16
    assert round(df.loc['2020-12-31']['open'], 2) == 218.89

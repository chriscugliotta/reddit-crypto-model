import shutil
from cc_idea.core.config import paths
from cc_idea.extractors.yahoo import get_prices


def test_get_prices():

    cache_path = paths.data / 'yahoo_finance_price_history' / 'symbol=MSFT'
    shutil.rmtree(cache_path, ignore_errors=True)

    df = get_prices('MSFT')
    df = df.set_index('date')

    assert round(df.loc['2018-12-31']['open'], 2) ==  97.95
    assert round(df.loc['2019-12-31']['open'], 2) == 153.84
    assert round(df.loc['2020-12-31']['open'], 2) == 219.86

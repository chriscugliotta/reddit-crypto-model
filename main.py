import logging
from datetime import datetime
from cc_idea.core.config import paths
from cc_idea.extractors.reddit import get_comments
from cc_idea.extractors.yahoo import get_prices
from cc_idea.reports.price_history.report import load
from cc_idea.utils.log_utils import initialize_logger
log = logging.getLogger('cc_idea')



if __name__ == '__main__':

    initialize_logger(paths.repo / 'log.log')
    log.info('Begin.')

    df_comments = get_comments(
        q='doge',
        start_date=datetime(2020, 1, 1, 0, 0, 0),
        end_date=datetime(2020, 2, 1, 0, 0, 0),
    )

    df_prices = get_prices(
        symbol='DOGE-USD',
    )

    df_report = load(df_prices, df_comments)

    log.info('Done.')

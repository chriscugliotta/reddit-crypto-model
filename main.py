import logging
from datetime import datetime
from cc_idea.core.config import paths
from cc_idea.extractors.reddit import load_comments, cache_comments
from cc_idea.extractors.yahoo import load_prices
from cc_idea.reports.price_history.report import load
from cc_idea.transformers.reddit import transform_comments
from cc_idea.utils.log_utils import initialize_logger
log = logging.getLogger('cc_idea')



if __name__ == '__main__':

    initialize_logger(paths.repo / 'log.log')
    log.info('Begin.')

    df_comments = load_comments(
        q='doge',
        start_date=datetime(2020, 1, 1, 0, 0, 0),
        end_date=datetime(2020, 1, 8, 0, 0, 0),
    )

    log.info('Done.')

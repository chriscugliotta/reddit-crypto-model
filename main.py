import logging
from datetime import date, datetime
from cc_idea.core.config import paths
from cc_idea.extractors.reddit import load_reddit, cache_reddit
from cc_idea.extractors.yahoo import load_prices
from cc_idea.transformers.reddit import transform_reddit
from cc_idea.utils.log_utils import initialize_logger
log = logging.getLogger('cc_idea')



if __name__ == '__main__':

    initialize_logger(paths.repo / 'log.log')
    log.info('Begin.')

    metas = cache_reddit(
        endpoint='comment',
        search=('q', 'cardano'),
        start_date=date(2020, 1, 1),
        end_date=date(2020, 2, 1),
    )

    df_transformed = transform_reddit(
        endpoint='comment',
        q='cardano',
        metas=metas,
    )

    log.info('Done.')

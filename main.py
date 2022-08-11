import logging
from datetime import date
from cc_idea.core.config import paths
from cc_idea.extractors.reddit import load_reddit, cache_reddit
from cc_idea.extractors.yahoo import load_prices
from cc_idea.transformers.reddit import transform_reddit
from cc_idea.utils.log_utils import initialize_logger
log = logging.getLogger('cc_idea')



def _extract_and_transform(endpoint: str, symbols: list, start_date: date, end_date: date):
    for symbol in symbols:
        caches = cache_reddit(
            endpoint=endpoint,
            q=symbol,
            start_date=start_date,
            end_date=end_date,
        )
        df_transformed = transform_reddit(
            endpoint=endpoint,
            q=symbol,
            caches=caches,
        )


if __name__ == '__main__':

    initialize_logger(paths.repo / 'log.log')
    log.info('Begin.')

    _extract_and_transform(
        endpoint='comment',
        symbols=['cardano'],
        start_date=date(2020, 1, 1),
        end_date=date(2021, 12, 1),
    )

    log.info('Done.')

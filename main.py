import logging
import yaml
from datetime import datetime
from pandas import DataFrame
from cc_idea.core.config import paths, config, symbols
from cc_idea.extractors.reddit import load_reddit, cache_reddit
from cc_idea.extractors.yahoo import load_prices
from cc_idea.transformers.reddit import transform_reddit
from cc_idea.utils.log_utils import initialize_logger
log = logging.getLogger('cc_idea')



def _extract_prices() -> DataFrame:
    """Extract prices via Yahoo Finance."""

    # Log.
    log.info('Begin.')

    # Abort.
    if 'yahoo' not in config['extractors']:
        return

    # Get in-scope symbols.
    yahoo_symbols = config['extractors']['yahoo']['symbols']
    if yahoo_symbols == 'all':
        yahoo_symbols = [x.yahoo_symbol for x in symbols]

    # Extract loop.
    for symbol in yahoo_symbols:
        load_prices(symbol)

    # Log.
    log.info('Done.')


def _extract_reddit(endpoint: str, search_key: str):
    """Extract Reddit comments or submissions."""

    # Abort.
    if 'reddit' not in config['extractors']:
        return
    if endpoint not in config['extractors']['reddit']['endpoints']:
        return
    if search_key not in config['extractors']['reddit']['queries']:
        return

    # Get in-scope queries.
    values = config['extractors']['reddit']['queries'][search_key]
    if values == 'all':
        values = [
            x
            for symbol in symbols
            for x in (symbol.reddit_q if search_key == 'q' else symbol.subreddits)
        ]
    values = sorted(list(set(values)))

    # Extract.
    log.info(f'Begin:  endpoint = {endpoint}, search_key = {search_key}, values = {len(values)}.')
    for value in values:
        cache_reddit(
            endpoint=endpoint,
            search={search_key: value},
            start_date=datetime.strptime(config['extractors']['reddit']['min_date'], '%Y-%m-%d').date(),
            end_date=datetime.strptime(config['extractors']['reddit']['max_date'], '%Y-%m-%d').date(),
        )
    log.info(f'Done.')



if __name__ == '__main__':

    # Log.
    initialize_logger(paths.repo / 'log.log')
    log.info('Begin.')
    log.info(f'config = \n{yaml.dump(config, indent=4)}')

    # Extract.
    _extract_prices()
    _extract_reddit('comment', 'q')
    _extract_reddit('comment', 'subreddit')
    _extract_reddit('submission', 'q')
    _extract_reddit('submission', 'subreddit')

    # Log.
    log.info('Done.')

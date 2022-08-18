import logging
import yaml
from datetime import datetime
from pandas import DataFrame
from cc_idea.core.config import paths, config, symbols
from cc_idea.extractors.reddit import RedditExtractor
from cc_idea.extractors.yahoo import YahooFinanceExtractor
from cc_idea.transformers.reddit import transform_reddit
from cc_idea.utils.log_utils import initialize_logger
log = logging.getLogger('cc_idea')



def _extract_prices() -> DataFrame:
    """Extract prices via Yahoo Finance."""

    # Log.
    log.info('Begin.')

    # Replace `all` shortcut with explicit list.
    def _get_yahoo_symbols(query: dict) -> list:
        if query['symbols'] == 'all':
            return [x.yahoo_symbol for x in symbols.values()]
        else:
            return query['symbols']

    # Loop and execute queries.
    for query in config['extractors']['yahoo'].get('queries', []):
        yahoo_symbols = _get_yahoo_symbols(query)
        caches = YahooFinanceExtractor().extract(yahoo_symbols)

    # Log.
    log.info('Done.')


def _extract_reddit():
    """Extract Reddit comments or submissions."""

    # Log.
    log.info('Begin.')

    # Parses config.
    def _get_filters(query: dict) -> list:
        if 'words' in query:
            filter_type = 'word'
            filter_values = query['words'] if query['words'] != 'all' else [x for _, symbol in symbols.items() for x in symbol.reddit_q]
        if 'subreddits' in query:
            filter_type = 'subreddit'
            filter_values = query['subreddits'] if query['subreddits'] != 'all' else [x for _, symbol in symbols.items() for x in symbol.subreddits]
        return [
            {
                'min_score': query.get('min_score'),
                filter_type: x,
            }
            for x in sorted(list(set(filter_values)))
        ]

    # Loop and execute queries.
    for query in config['extractors']['reddit'].get('queries', []):
        for filter in _get_filters(query):
            RedditExtractor().extract(
                endpoint=query['endpoint'],
                filters=filter,
                min_date=datetime.strptime(config['extractors']['reddit']['min_date'], '%Y-%m-%d').date(),
                max_date=datetime.strptime(config['extractors']['reddit']['max_date'], '%Y-%m-%d').date(),
            )

    # Log.
    log.info('Done.')



if __name__ == '__main__':

    # Log.
    initialize_logger(paths.repo / 'log.log')
    log.info('Begin.')
    log.info(f'config = \n{yaml.dump(config, indent=4)}')

    # Extract.
    _extract_prices()
    _extract_reddit()

    # Log.
    log.info('Done.')

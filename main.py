import json
import logging
import yaml
from datetime import datetime
from pandas import DataFrame
from typing import Dict
from cc_idea.core.cache import DateCache, DateRangeCache
from cc_idea.core.config import paths, config, symbols
from cc_idea.extractors.reddit import RedditExtractor
from cc_idea.extractors.yahoo import YahooFinanceExtractor
from cc_idea.transformers.sentiment import SentimentTransformer
from cc_idea.utils.log_utils import initialize_logger
log = logging.getLogger('cc_idea')



def extract_prices() -> DataFrame:
    """Extracts prices via Yahoo Finance."""

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
        df = YahooFinanceExtractor().extract(yahoo_symbols, read=True)

    # Log, return.
    log.info('Done.')
    return df


def extract_reddit() -> Dict[str, DateCache]:
    """Extracts Reddit comments and submissions."""

    # Log.
    log.info('Begin.')

    # Parses config, pulls RedditExtractor args.
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
    results = {}
    for query in config['extractors']['reddit'].get('queries', []):
        for filters in _get_filters(query):
            key = RedditExtractor()._get_cache_key(query['endpoint'], filters)
            results[key] = RedditExtractor().extract(
                endpoint=query['endpoint'],
                filters=filters,
                min_date=datetime.strptime(config['extractors']['reddit']['min_date'], '%Y-%m-%d').date(),
                max_date=datetime.strptime(config['extractors']['reddit']['max_date'], '%Y-%m-%d').date(),
            )

    # Log, return.
    log.info('Done.')
    return results


def transform_sentiment(data: dict) -> Dict[str, DateRangeCache]:
    """Performs sentiment analysis on Reddit comments and submissions."""

    # Log.
    log.info('Begin.')

    # Calculate sentiments.
    results = {}
    for key, caches in data['reddit'].items():
        _key = json.loads(key)
        results[key] = SentimentTransformer().transform(
            endpoint=_key['endpoint'],
            filters=_key['filters'],
            caches=caches,
            chunk_size=config['transformers']['sentiment']['chunk_size'],
        )

    # Log, return.
    log.info('Done.')
    return results



if __name__ == '__main__':

    # Log.
    initialize_logger(paths.repo / 'log.log')
    log.info('Begin.')
    log.info(f'config = \n{yaml.dump(config, indent=4)}')

    # Extract.
    data = {}
    data['yahoo'] = extract_prices()
    data['reddit'] = extract_reddit()
    data['sentiment'] = transform_sentiment(data)

    # Log.
    log.info('Done.')

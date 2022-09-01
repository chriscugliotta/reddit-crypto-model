import logging
import yaml
from pandas import DataFrame
from typing import Dict, List, Tuple
from rcm.core.cache import DateCache, DateRangeCache
from rcm.core.config import paths, config
from rcm.extractors.reddit import RedditExtractor
from rcm.extractors.yahoo import YahooFinanceExtractor
from rcm.transformers.aggregation import AggregationTransformer
from rcm.transformers.densify import DensifyTransformer
from rcm.transformers.sentiment import SentimentTransformer
from rcm.utils.log_utils import initialize_logger
log = logging.getLogger('rcm')



def extract_yahoo() -> DataFrame:
    """Extracts price history via Yahoo Finance."""
    return YahooFinanceExtractor().extract(
        symbols=config.extractors.yahoo.symbols,
        read=True,
    )


def extract_reddit(endpoint: str) -> Dict[Tuple[str, str], List[DateCache]]:
    """Extracts Reddit comments or submissions via Pushshift API."""
    return {
        query['search']: RedditExtractor().extract(**query)
        for query in config.extractors.reddit.queries
        if query['endpoint'] == endpoint
    }


def transform_sentiment(data: Dict, endpoint: str) -> Dict[Tuple[str, str], DateRangeCache]:
    """Performs sentiment analysis on Reddit data via VaderSentiment and TextBlob."""
    return {
        query['search']: SentimentTransformer().transform(
            endpoint=query['endpoint'],
            search=query['search'],
            min_score=query['min_score'],
            caches=data[f'reddit_{endpoint}s'][query['search']],
        )
        for query in config.extractors.reddit.queries
        if query['endpoint'] == endpoint
    }


def main():

    # Log.
    log.info('Begin.')
    log.info(f'config = \n{yaml.dump(config._yaml, indent=4)}')

    # Extract.
    data = {}
    data['yahoo_finance_price_history'] = extract_yahoo()
    data['reddit_comments'] = extract_reddit('comment')
    data['reddit_submissions'] = extract_reddit('submission')

    # Transform.
    data['reddit_comments_sentiment'] = transform_sentiment(data, 'comment')
    data['reddit_submissions_sentiment'] = transform_sentiment(data, 'submission')
    data['reddit_aggregations'] = AggregationTransformer().transform(data)
    data['features_dense'] = DensifyTransformer().transform(data)

    # Log.
    log.info('Done.')



if __name__ == '__main__':
    initialize_logger(paths.repo / 'log.log')
    try:
        main()
    except:
        log.exception('Error.')
        raise

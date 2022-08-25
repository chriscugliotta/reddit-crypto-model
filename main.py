import logging
import yaml
from pandas import DataFrame
from typing import Dict, List, Tuple
from cc_idea.core.cache import DateCache, DateRangeCache
from cc_idea.core.config import paths, config
from cc_idea.extractors.reddit import RedditExtractor
from cc_idea.extractors.yahoo import YahooFinanceExtractor
from cc_idea.transformers.sentiment import SentimentTransformer
from cc_idea.utils.log_utils import initialize_logger
log = logging.getLogger('cc_idea')



def extract_yahoo() -> DataFrame:
    """Extracts price history via Yahoo Finance."""
    return YahooFinanceExtractor().extract(
        symbols=config.extractors.yahoo.symbols,
        read=True,
    )


def extract_reddit(endpoint: str) -> Dict[Tuple[str, str], List[DateCache]]:
    """Extracts Reddit comments and submissions via Pushshift API."""
    return {
        query['search']: RedditExtractor().extract(**query)
        for query in config.extractors.reddit.queries
        if query['endpoint'] == endpoint
    }


def transform_sentiment(data: Dict, endpoint: str) -> Dict[str, DateRangeCache]:
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



if __name__ == '__main__':

    # Log.
    initialize_logger(paths.repo / 'log.log')
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

    # Log.
    log.info('Done.')

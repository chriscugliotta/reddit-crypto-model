import shutil
from datetime import date
from cc_idea.core.config import paths
from cc_idea.extractors.reddit import RedditExtractor
from cc_idea.transformers.sentiment import SentimentTransformer



def test_sentiment_transformer():
    """Runs an end-to-end test for RedditExtractor and SentimentTransformer."""

    # Clean up.
    cache_paths = [
        paths.data / 'reddit_comments' / 'min_score=null' / 'word=zoltan',
        paths.data / 'reddit_comments_sentiment' / 'min_score=null' / 'word=zoltan',
    ]
    for path in cache_paths:
        shutil.rmtree(path, ignore_errors=True)

    # Extract.
    caches = RedditExtractor().extract(
        endpoint='comment',
        filters={'min_score': None, 'word': 'zoltan'},
        min_date=date(2020, 1, 1),
        max_date=date(2020, 1, 3),
    )

    # Transform.
    range_cache = SentimentTransformer().transform(
        endpoint='comment',
        filters={'min_score': None, 'word': 'zoltan'},
        caches=caches,
        chunk_size=0,
    )

    # Validate.
    df = range_cache.load()
    cache_files = list(cache_paths[1].glob('*.parquet'))
    assert len(cache_files) == 1
    assert cache_files[0].name == f'min_date=2020-01-01, max_date=2020-01-03.snappy.parquet'
    assert len(df) == 60
    assert int(df['score'].sum()) == 321
    assert int(df['positive'].sum()) == 7
    assert int(df['polarity'].sum()) == 7
    assert df['created_date'].min() == date(2020, 1, 1)
    assert df['created_date'].max() == date(2020, 1, 3)

    # Clean up.
    for path in cache_paths:
        shutil.rmtree(path)

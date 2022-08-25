import shutil
from datetime import date
from cc_idea.core.config import paths
from cc_idea.extractors.reddit import RedditExtractor
from cc_idea.transformers.sentiment import SentimentTransformer



def test_sentiment_transformer():
    """Runs an end-to-end test for RedditExtractor and SentimentTransformer."""

    # Clean up.
    cache_prefixes = [
        RedditExtractor()._get_cache_prefix('comment', ('word', 'zoltan'), None),
        SentimentTransformer()._get_cache_prefix('comment', ('word', 'zoltan'), None),
    ]
    for path in cache_prefixes:
        shutil.rmtree(path, ignore_errors=True)

    # Extract.
    caches = RedditExtractor().extract(
        endpoint='comment',
        search=('word', 'zoltan'),
        min_score=None,
        min_date=date(2020, 1, 1),
        max_date=date(2020, 1, 3),
    )

    # Transform.
    range_cache = SentimentTransformer().transform(
        endpoint='comment',
        search=('word', 'zoltan'),
        min_score=None,
        caches=caches,
        chunk_size=0,
    )

    # Validate parqet file.
    cache_files = list(cache_prefixes[1].glob('*.parquet'))
    assert len(cache_files) == 1
    assert cache_files[0].name == f'min_date=2020-01-01, max_date=2020-01-03.snappy.parquet'

    # Validate dataframe.
    df = range_cache.load()
    assert len(df) == 60
    assert int(df['score'].sum()) == 321
    assert int(df['positive'].sum()) == 7
    assert int(df['polarity'].sum()) == 7
    assert df['created_date'].min() == date(2020, 1, 1)
    assert df['created_date'].max() == date(2020, 1, 3)

    # Clean up.
    for path in cache_prefixes:
        shutil.rmtree(path)

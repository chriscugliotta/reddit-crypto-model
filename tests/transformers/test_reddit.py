import shutil
from datetime import date
from cc_idea.core.config import paths
from cc_idea.extractors.reddit import cache_reddit
from cc_idea.transformers.reddit import transform_reddit



def test_transform_reddit():
    """Runs an end-to-end test for Reddit extractor and transformer."""

    # Clean up.
    cache_paths = [
        paths.data / 'reddit_comments' / 'min_score=null' / 'word=zoltan',
        paths.data / 'reddit_comments_aggregated' / 'min_score=null' / 'word=zoltan',
    ]
    for path in cache_paths:
        shutil.rmtree(path, ignore_errors=True)

    # Extract.
    caches = cache_reddit(
        endpoint='comment',
        min_date=date(2020, 1, 1),
        max_date=date(2020, 1, 3),
        filters={'min_score': None, 'word': 'zoltan'},
    )

    # Transform.
    df = transform_reddit(
        endpoint='comment',
        filters={'min_score': None, 'word': 'zoltan'},
        caches=caches,
        chunk_size=0,
    )

    # Validate.
    cache_files = list(cache_paths[1].glob('*.parquet'))
    assert len(cache_files) == 1
    assert cache_files[0].name == f'min_date=2020-01-01, max_date=2020-01-03.snappy.parquet'
    assert len(df) == 3
    assert df.iloc[0]['created_date'] == date(2020, 1, 1)
    assert df.iloc[0]['num_comments'] == 24

    # Clean up.
    for path in cache_paths:
        shutil.rmtree(path)

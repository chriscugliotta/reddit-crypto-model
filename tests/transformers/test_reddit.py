import shutil
from datetime import date
from cc_idea.core.config import paths
from cc_idea.extractors.reddit import cache_reddit
from cc_idea.transformers.reddit import transform_reddit



def test_transform_reddit():
    """Runs an end-to-end test for Reddit extractor and transformer."""

    # Clean up.
    cache_paths = [
        paths.data / 'reddit_comments' / 'q=zoltan',
        paths.data / 'reddit_comments_aggregated' / 'q=zoltan',
    ]
    for path in cache_paths:
        shutil.rmtree(path, ignore_errors=True)

    # Extract.
    metas = cache_reddit(
        endpoint='comment',
        search=('q', 'zoltan'),
        start_date=date(2020, 1, 1),
        end_date=date(2020, 1, 4),
    )

    # Transform.
    df = transform_reddit(
        endpoint='comment',
        q='zoltan',
        metas=metas,
        chunk_size=0,
    )

    # Validate.
    cache_files = list(cache_paths[1].glob('*.parquet'))
    assert len(cache_files) == 1
    assert cache_files[0].name == f'min_date=2020-01-01, max_date=2020-01-03.snappy.parquet'
    assert df.shape[0] == 3
    assert df.iloc[0]['created_date'] == date(2020, 1, 1)
    assert df.iloc[0]['num_comments'] == 24

    # Clean up.
    for path in cache_paths:
        shutil.rmtree(path)

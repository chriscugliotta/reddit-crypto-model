import shutil
from datetime import datetime
from cc_idea.core.config import paths
from cc_idea.extractors.reddit import cache_reddit
from cc_idea.transformers.reddit import transform_comments



def test_transform_comments():

    cache_paths = [
        paths.data / 'reddit_comments' / 'q=zoltan',
        paths.data / 'reddit_comments_transformed' / 'q=zoltan',
    ]
    for path in cache_paths:
        shutil.rmtree(path, ignore_errors=True)

    metas = cache_reddit(
        endpoint='comment',
        search=('q', 'zoltan'),
        start_date=datetime(2020, 1, 1, 0, 0, 0),
        end_date=datetime(2020, 1, 4, 0, 0, 0),
    )

    df = transform_comments(
        q='zoltan',
        metas=metas,
    )

    assert df.shape[0] == 3
    assert df.iloc[0]['created_date'] == datetime(2020, 1, 1, 0, 0, 0)
    assert df.iloc[0]['num_comments'] == 24

    for path in cache_paths:
        shutil.rmtree(path)

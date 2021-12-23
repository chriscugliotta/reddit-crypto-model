import shutil
from datetime import date
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
        start_date=date(2020, 1, 1),
        end_date=date(2020, 1, 4),
    )

    df = transform_comments(
        q='zoltan',
        metas=metas,
    )

    assert df.shape[0] == 3
    assert df.iloc[0]['created_date'] == date(2020, 1, 1)
    assert df.iloc[0]['num_comments'] == 24

    for path in cache_paths:
        shutil.rmtree(path)

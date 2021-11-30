import gzip
import json
import logging
import pandas as pd
from datetime import datetime, timedelta
from pandas import DataFrame
from typing import Dict, List
from cc_idea.core.config import paths
from cc_idea.utils.request_utils import get_request
log = logging.getLogger(__name__)



def get_comments(q: str, start_date: datetime, end_date: datetime) -> DataFrame:
    """
    Returns all comments posted between `start_date` and `end_date` mentioning the word `q`.

    Note:
        As a convenience, this function dumps all API responses into a singular dataframe.
    """

    # Log.
    log.debug(f'Begin with q = {q}, start_date = {start_date:%Y-%m-%d}, end_date = {end_date:%Y-%m-%d}.')

    # Load one day at a time.
    data = []
    for i in range((end_date - start_date).days + 1):
        data += _get_comments_on_date(q, start_date + timedelta(days=i))

    # Convert responses into dataframes.
    frames = []
    for result in data:
        frame = pd.DataFrame(result['response']['json']['data'])
        frames += [frame]

    # Combine dataframes.
    df = pd.concat(frames, ignore_index=True)

    # Log, return.
    log.debug(f'Done with q = {q}, start_date = {start_date:%Y-%m-%d}, end_date = {end_date:%Y-%m-%d}, rows = {df.shape[0]:,}.')
    return df


def _get_comments_on_date(q: str, target_date: datetime) -> List[Dict]:
    """
    Returns all comments posted on `target_date` mentioning the word `q`.

    Note:
        This function caches all API responses.  A separate local JSON file is created for every
        `(target_date, q)` combination.  If a given request is already cached, we skip the API call
        and return the cached result instead.
    """

    # Get cache path for upcoming request.
    cache_path = paths.data / 'reddit_comments' / f'target_date={target_date.strftime("%Y-%m-%d")}' / f'q={q}' / '0.json.gz'
    cached = cache_path.is_file()

    # If result is not cached, hit the API and cache the result.
    if not cached:
        data = _load_comments(q, target_date, target_date + timedelta(days=1))
        cache_path.parent.mkdir(parents=True)
        with gzip.open(cache_path, 'wt') as file:
            json.dump(data, file)

    # Get result from cache.
    with gzip.open(cache_path, 'rt') as file:
        data = json.load(file)

    # Log, return.
    log.debug(f'Done with q = {q}, target_date = {target_date:%Y-%m-%d}, rows = {sum(x["response"]["rows"] for x in data):,}, cached = {cached}.')
    return data


def _load_comments(q: str, start_date: datetime, end_date: datetime, max_iterations: int = 3600) -> List[Dict]:
    """
    Iteratively queries the Pushshift API, and returns a list all comments posted between
    `start_date` and `end_date` mentioning the word `q`.

    Note:
        Pushshift will return (at most) 100 comments per request.  To overcome this limitation, we
        must iteratively pull the data.  Each iteration, we slide our search window from left-to-
        right, until the entire interval has been searched.

    References:
        Pushshift API:
        https://github.com/pushshift/api
    """

    results = []
    batch_min_date = start_date

    for i in range(max_iterations):

        # Pull batch i.
        params = {
            'q': q,
            'after': int(batch_min_date.timestamp()),
            'before': int(end_date.timestamp()),
            'size': 100,
            'sort_type': 'created_utc',
            'sort': 'asc',
        }
        result = get_request('https://api.pushshift.io/reddit/search/comment', params, i)
        batch = result['response']['json']['data']

        # If batch is empty, our query is complete.
        if len(batch) == 0:
            log.debug(f'i = {i}, batch = {len(batch)}, done.')
            return results

        # Get minimum and maximum dates in batch.
        batch_min_date = datetime.fromtimestamp(min([x['created_utc'] for x in batch]))
        batch_max_date = datetime.fromtimestamp(max([x['created_utc'] for x in batch]))

        # Estimate total iterations to complete query.
        total_distance = end_date - start_date
        distance_per_iteration = (batch_max_date - start_date) / (i + 1)
        estimated_iterations = total_distance / distance_per_iteration

        # Add batch to results.
        results.append(result)
        log.debug('i = {}, total = {:,}, batch = {:,}, date = {}, batch_min = {}, batch_max = {}, estimated = {:.2f}'.format(
            i,
            sum(x['response']['rows'] for x in results),
            len(batch),
            batch_min_date.strftime('%Y-%m-%d'),
            batch_min_date.strftime('%H:%M:%S'),
            batch_max_date.strftime('%H:%M:%S'),
            estimated_iterations,
        ))
        i += 1
        batch_min_date = batch_max_date

        # If maximum number iterations exceeded, stop early.
        if i == max_iterations - 1:
            log.warning(f'i = {i}, max iterations exceeded.')
            return results

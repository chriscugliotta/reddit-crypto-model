import gzip
import json
import logging
import pandas as pd
from datetime import date, datetime, timedelta
from pandas import DataFrame
from typing import List, Tuple
from cc_idea.core.config import paths
from cc_idea.utils.date_utils import date_to_datetime
from cc_idea.utils.request_utils import get_request
log = logging.getLogger(__name__)



def load_reddit(endpoint: str, search: Tuple[str, str], start_date: date, end_date: date, metas: List[dict] = [], columns: List[str] = None) -> DataFrame:
    """
    Loads all comments (or submissions) posted within the given search filters.

    Returns:  A dataframe containing all API responses.
    """
    log.debug(f'Begin with endpoint = {endpoint}, {search[0]} = {search[1]}, start_date = {start_date}, end_date = {end_date}, metas = {len(metas)}.')
    metas = cache_reddit(endpoint, search, start_date, end_date) if metas == [] else metas
    frames = []
    for meta in metas:
        with gzip.open(meta['path'], 'rt') as file:
            data = json.load(file)
        for result in data:
            frame = pd.DataFrame(result['response']['json']['data'], columns=columns)
            frames += [frame]
    df = pd.concat(frames, ignore_index=True)
    log.debug(f'Done with endpoint = {endpoint}, {search[0]} = {search[1]}, start_date = {start_date}, end_date = {end_date}, metas = {len(metas)}, rows = {df.shape[0]:,}.')
    return df


def cache_reddit(endpoint: str, search: Tuple[str, str], start_date: date, end_date: date) -> List[dict]:
    """
    Caches all comments (or submissions) posted within the given search filters.

    Returns:  A list of cache metadata.
    """

    # Log.
    log.debug(f'Begin with endpoint = {endpoint}, {search[0]} = {search[1]}, start_date = {start_date}, end_date = {end_date}.')

    # Never load future dates.
    # Never load current date (to prevent stale snapshot in cache).
    end_date = min(end_date, date.today())

    # Cache one day at a time.
    metas = [
        _cache_reddit_date(endpoint, search, start_date + timedelta(days=i))
        for i in range((end_date - start_date).days)
    ]

    # Log, return.
    log.debug(f'Done with endpoint = {endpoint}, {search[0]} = {search[1]}, start_date = {start_date}, end_date = {end_date}, metas = {len(metas):,}.')
    return metas


def _cache_reddit_date(endpoint: str, search: Tuple[str, str], target_date: date) -> dict:
    """
    Caches all comments (or submissions) posted on `target_date` within the given search filters.

    Note:
        This function caches all API responses.  A separate local JSON file is created for every
        `(target_date, search)` combination.  If a given request is already cached, we skip the API
        call.
    """

    # Get cache path for upcoming request.
    cache_path = paths.data / f'reddit_{endpoint}s' / f'{search[0]}={search[1]}' / f'year={target_date.strftime("%Y")}' / f'month={target_date.strftime("%m")}' / f'day={target_date.strftime("%d")}' / '0.json.gz'

    # If result is not cached, hit the API and cache the result.
    if not cache_path.is_file():
        data = _load_reddit(endpoint, search, date_to_datetime(target_date), date_to_datetime(target_date + timedelta(days=1)))
        cache_path.parent.mkdir(parents=True)
        with gzip.open(cache_path, 'wt') as file:
            json.dump(data, file)
        log.debug(f'Done with endpoint = {endpoint}, {search[0]} = {search[1]}, target_date = {target_date}, rows = {sum(x["response"]["rows"] for x in data):,}.')

    # Return cache file metadata.
    return {
        'search': search,
        'date': target_date,
        'path': cache_path,
    }


def _load_reddit(endpoint: str, search: Tuple[str, str], start_date: datetime, end_date: datetime, max_iterations: int = 3600) -> List[dict]:
    """
    Iteratively queries the Pushshift API, and returns all comments (or submissions) posted within
    the given search filters.

    Note:
        Pushshift will return (at most) 100 comments per request.  To overcome this limitation, we
        must iteratively pull the data.  Each iteration, we slide our search window from left-to-
        right, until the entire interval has been searched.

    References:
        Pushshift API:
        https://github.com/pushshift/api
    """
    # TODO:  Be careful.  `timestamp` and `fromtimestamp` functions will assume local machine's timezone.
    # TODO:  On non-EST machine, will need to explicitly declare US/Eastern during all epoch conversions.

    results = []
    batch_min_date = start_date

    for i in range(max_iterations):

        # Pull batch i.
        params = {
            search[0]: search[1],
            'after': int(batch_min_date.timestamp()),
            'before': int(end_date.timestamp()),
            'size': 100,
            'sort_type': 'created_utc',
            'sort': 'asc',
        }
        result = get_request(f'https://api.pushshift.io/reddit/search/{endpoint}', params, i)
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

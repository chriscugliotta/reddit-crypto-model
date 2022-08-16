import logging
import pandas as pd
from datetime import date, datetime, timedelta
from pandas import DataFrame
from pathlib import Path
from typing import List
from cc_idea.core.cache import DateCache
from cc_idea.core.config import paths
from cc_idea.utils.date_utils import date_to_datetime
from cc_idea.utils.request_utils import get_request
log = logging.getLogger(__name__)



def load_reddit(endpoint: str, min_date: date, max_date: date, filters: dict, caches: List[DateCache] = [], columns: List[str] = None) -> DataFrame:
    """
    Loads all comments (or submissions) posted within the given search filters.

    Args:
        endpoint (str):
            Pushshift API endpoint.  Either `comment` or `submission`.

        min_date, max_date (date):
            Search time range
            All comments (or submissions) posted between [min_date, max_date] will be returned.

        filters (Dict[str, str]):
            Additional search filters.
            Examples:  `{word: doge}` or `{min_score: 2, subreddit: dogelore}`.

        caches (List[DateCache]):
            List of cache objects.  If provided, the API is not hit.  Instead, we only load the
            given cache files.

        columns (List[str]):
            List of JSON attributes (from API response) to include as dataframe columns.  If
            omitted, all attributes are included.

    Returns:
        DataFrame:  Contains all API responses.
    """
    log.debug(f'Begin with endpoint = {endpoint}, min_date = {min_date}, max_date = {max_date}, filters = {filters}, caches = {len(caches)}.')
    caches = cache_reddit(endpoint, min_date, max_date, filters) if caches == [] else caches
    frames = [
        pd.DataFrame(result['response']['json']['data'], columns=columns)
        for cache in caches
        for result in cache.load()
    ]
    df = pd.concat(frames, ignore_index=True)
    log.debug(f'Done with endpoint = {endpoint}, min_date = {min_date}, max_date = {max_date}, caches = {len(caches)}, rows = {len(df)}.')
    return df


def cache_reddit(endpoint: str, min_date: date, max_date: date, filters: dict) -> List[DateCache]:
    """
    Caches all comments (or submissions) posted within the given search filters.

    Returns:
        List[DateCache]:  List of cache objects.
    """

    # Log.
    log.debug(f'Begin with endpoint = {endpoint}, min_date = {min_date}, max_date = {max_date}, filters = {filters}.')

    # Never load future dates.
    # Never load current date (to prevent stale snapshot in cache).
    max_date = min(max_date, date.today())

    # Cache one day at a time.
    caches = [
        _cache_reddit_date(endpoint, min_date + timedelta(days=i), filters)
        for i in range((max_date - min_date).days + 1)
    ]

    # Log, return.
    log.debug(f'Done with endpoint = {endpoint}, min_date = {min_date}, max_date = {max_date}, filters = {filters}, caches = {len(caches):,}.')
    return caches


def _cache_reddit_date(endpoint: str, target_date: date, filters: dict) -> DateCache:
    """
    Caches all comments (or submissions) posted on `target_date` within the given search filters.

    Note:
        This function caches all API responses.  A separate local JSON file is created for every
        `(target_date, filters)` combination.  If a given request is already cached, we skip the API
        call.
    """

    # Get cache object for upcoming request.
    cache = DateCache(target_date, _get_cache_prefix(endpoint, filters))

    # If result is not cached, hit the API and cache the result.
    if not cache.path.is_file():
        data = _load_reddit(endpoint, date_to_datetime(target_date), date_to_datetime(target_date + timedelta(days=1)), filters)
        cache.save(data)
        log.debug(f'Done with endpoint = {endpoint}, target_date = {target_date}, filters = {filters}, rows = {sum(x["response"]["rows"] for x in data):,}.')

    # Return cache object.
    return cache


def _get_cache_prefix(endpoint: str, filters: dict) -> Path:
    """Returns cache path prefix for given endpoint and filter set."""
    min_score = filters.get('min_score') if filters.get('min_score') else 'null'
    suffix = ', '.join(f'{k}={v}' for k, v in filters.items() if k != 'min_score')
    return paths.data / f'reddit_{endpoint}s' / f'min_score={min_score}' / suffix


def _load_reddit(endpoint: str, min_date: datetime, max_date: datetime, filters: dict, max_iterations: int = 3600) -> List[dict]:
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
    batch_min_date = min_date

    for i in range(max_iterations):

        # Pull batch i.
        params = {
            'score': '>' + str(filters['min_score']) if filters.get('min_score') else None,
            'q': filters.get('word'),
            'subreddit': filters.get('subreddit'),
            'after': int(batch_min_date.timestamp()),
            'before': int(max_date.timestamp()),
            'size': 100,
            'sort_type': 'created_utc',
            'sort': 'asc',
        }
        params = {k: v for k, v in params.items() if v is not None}
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
        total_distance = max_date - min_date
        distance_per_iteration = (batch_max_date - min_date) / (i + 1)
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

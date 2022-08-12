import logging
import pandas as pd
from datetime import date, datetime, timedelta
from pandas import DataFrame
from typing import List
from cc_idea.core.cache import DateCache
from cc_idea.core.config import paths
from cc_idea.utils.date_utils import date_to_datetime
from cc_idea.utils.request_utils import get_request
log = logging.getLogger(__name__)



def load_reddit(endpoint: str, search: dict, start_date: date, end_date: date, caches: List[DateCache] = [], columns: List[str] = None) -> DataFrame:
    """
    Loads all comments (or submissions) posted within the given search filters.

    Args:
        endpoint (str):
            Pushshift API endpoint.  Either `comment` or `submission`.

        search (Tuple[str, str]):
            Search term.
            All comments (or submissions) containing this term will be returned.
            Examples:  `{q: doge}` or `{subreddit: CryptoCurrency, score: >20}`.

        start_date, end_date (date):
            Search time range
            All comments (or submissions) posted between [start_date, end_date) will be returned.

        caches (List[DateCache]):
            List of cache objects.  If provided, the API is not hit.  Instead, we only load the
            given cache files.

        columns (List[str]):
            List of JSON attributes (from API response) to include as dataframe columns.  If
            omitted, all attributes are included.

    Returns:
        DataFrame:  Contains all API responses.
    """
    log.debug(f'Begin with endpoint = {endpoint}, search = {search}, start_date = {start_date}, end_date = {end_date}, caches = {len(caches)}.')
    caches = cache_reddit(endpoint, search, start_date, end_date) if caches == [] else caches
    frames = [
        pd.DataFrame(result['response']['json']['data'], columns=columns)
        for cache in caches
        for result in cache.load()
    ]
    df = pd.concat(frames, ignore_index=True)
    log.debug(f'Done with endpoint = {endpoint}, search = {search}, start_date = {start_date}, end_date = {end_date}, caches = {len(caches)}, rows = {len(df)}.')
    return df


def cache_reddit(endpoint: str, search: dict, start_date: date, end_date: date) -> List[DateCache]:
    """
    Caches all comments (or submissions) posted within the given search filters.

    Returns:
        List[DateCache]:  List of cache objects.
    """

    # Log.
    log.debug(f'Begin with endpoint = {endpoint}, search = {search}, start_date = {start_date}, end_date = {end_date}.')

    # Never load future dates.
    # Never load current date (to prevent stale snapshot in cache).
    end_date = min(end_date, date.today())

    # Cache one day at a time.
    caches = [
        _cache_reddit_date(endpoint, search, start_date + timedelta(days=i))
        for i in range((end_date - start_date).days + 1)
    ]

    # Log, return.
    log.debug(f'Done with endpoint = {endpoint}, search = {search}, start_date = {start_date}, end_date = {end_date}, caches = {len(caches):,}.')
    return caches


def _cache_reddit_date(endpoint: str, search: dict, target_date: date) -> DateCache:
    """
    Caches all comments (or submissions) posted on `target_date` within the given search filters.

    Note:
        This function caches all API responses.  A separate local JSON file is created for every
        `(target_date, q)` combination.  If a given request is already cached, we skip the API call.
    """

    # Get cache object for upcoming request.
    cache = DateCache(
        date=target_date,
        prefix=paths.data / f'reddit_{endpoint}s' / ', '.join(f'{k}={v}' for k, v in search.items()),
        suffix='.json.gz',
    )

    # If result is not cached, hit the API and cache the result.
    if not cache.path.is_file():
        data = _load_reddit(endpoint, search, date_to_datetime(target_date), date_to_datetime(target_date + timedelta(days=1)))
        cache.save(data)
        log.debug(f'Done with endpoint = {endpoint}, search = {search}, target_date = {target_date}, rows = {sum(x["response"]["rows"] for x in data):,}.')

    # Return cache object.
    return cache


def _load_reddit(endpoint: str, search: dict, start_date: datetime, end_date: datetime, max_iterations: int = 3600) -> List[dict]:
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
            # TODO:  Figure out how to parameterize:  'score': '>9',
            'after': int(batch_min_date.timestamp()),
            'before': int(end_date.timestamp()),
            'size': 100,
            'sort_type': 'created_utc',
            'sort': 'asc',
        }
        params = {**search, **params}
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

import json
import logging
import pandas
from datetime import date, datetime, timedelta
from pandas import DataFrame
from pathlib import Path
from typing import Dict, List
from cc_idea.core.cache import DateCache
from cc_idea.core.config import paths
from cc_idea.core.extractor import Extractor
from cc_idea.utils.date_utils import date_to_datetime, path_to_date
from cc_idea.utils.request_utils import get_request
log = logging.getLogger(__name__)



class RedditExtractor(Extractor):

    def __init__(self):
        self.schema: Dict[str, str] = {
            'id': 'string',
            'created_utc': 'float64',
            'author': 'string',
            'subreddit': 'string',
            'title': 'string',
            'body': 'string',
            'score': 'int64',
        }
        self.unique_key: List[str] = ['id']

    def extract(self, endpoint: str, filters: dict, min_date: date, max_date: date, read: bool = False) -> List[DateCache]:
        """
        Extracts (and caches) all comments (or submissions) posted within the given search filters.

        Args:
            endpoint (str):
                Pushshift API endpoint.  Either `comment` or `submission`.

            filters (Dict[str, str]):
                Search filters.
                Examples:  `{word: doge}` or `{min_score: 2, subreddit: dogelore}`.

            min_date, max_date (date):
                Search time range
                All comments (or submissions) posted between [min_date, max_date] will be returned.

            read (bool):
                If true, dataframe is returned instead of cache objects.

        Returns:
            List[DateCache]:  List of cache objects.
        """

        # Log.
        log.debug(f'Begin with endpoint = {endpoint}, filters = {filters}, min_date = {min_date}, max_date = {max_date}.')

        # Never load future dates.
        # Never load current date (to prevent stale snapshot in cache).
        max_date = min(max_date, date.today())

        # Extract (and cache) one day at a time.
        caches = [
            self._extract_date(endpoint, filters, min_date + timedelta(days=i))
            for i in range((max_date - min_date).days + 1)
        ]

        # Log, return.
        log.debug(f'Done with endpoint = {endpoint}, filters = {filters}, min_date = {min_date}, max_date = {max_date}, caches = {len(caches):,}.')
        return caches if not read else self.read(endpoint, filters, min_date, max_date, caches)

    def _extract_date(self, endpoint: str, filters: dict, target_date: date) -> DateCache:
        """
        Extracts all comments (or submissions) posted on `target_date` within the given search filters.

        Note:
            This function caches all API responses.  A separate local JSON file is created for every
            `(filters, target_date)` combination.  If a given request is already cached, we skip the
            API call.
        """

        # Get cache object for upcoming request.
        cache = DateCache(target_date, self._get_cache_prefix(endpoint, filters))

        # If result is not cached, hit the API and cache the result.
        if not cache.path.is_file():
            data = self._extract_api(endpoint, filters, date_to_datetime(target_date), date_to_datetime(target_date + timedelta(days=1)))
            cache.save(data)
            log.debug(f'Done with endpoint = {endpoint}, filters = {filters}, target_date = {target_date}, rows = {sum(x["response"]["rows"] for x in data):,}.')

        # Return cache object.
        return cache

    def _extract_api(self, endpoint: str, filters: dict, min_date: datetime, max_date: datetime) -> List[dict]:
        """
        Iteratively queries the Pushshift API, and returns all comments (or submissions) posted
        within the given search filters.

        Note:
            Pushshift will return (at most) 100 comments per request.  To overcome this limitation,
            we must iteratively pull the data.  Each iteration, we slide our search window from
            left-to-right, until the entire interval has been searched.

        References:
            Pushshift API:
            https://github.com/pushshift/api
        """
        # TODO:  Be careful.  `timestamp` and `fromtimestamp` functions will assume local machine's timezone.
        # TODO:  On non-EST machine, will need to explicitly declare US/Eastern during all epoch conversions.

        results = []
        batch_min_date = min_date
        max_iterations = 1000

        for i in range(max_iterations):

            # Pull batch i.
            params = {
                'score': '>' + str(filters['min_score'] - 1) if filters.get('min_score') else None,
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

    def _read(self, endpoint: str, filters: dict, min_date: date = None, max_date: date = None, caches: List[DateCache] = None) -> DataFrame:
        """Reads previously-cached data into a dataframe."""

        # Log.
        log.debug(f'Begin with endpoint = {endpoint}, filters = {filters}, min_date = {min_date}, max_date = {max_date}, caches = {0 if caches is None else len(caches)}.')

        # If cache targets not provided, search for them.
        if caches is None:
            prefix = self._get_cache_prefix(endpoint, filters)
            caches = [
                DateCache(path_to_date(x), prefix)
                for x in prefix.rglob('*.json.gz')
                if (min_date is None or path_to_date(x) >= min_date) and (max_date is None or path_to_date(x) <= max_date)
            ]

        # Read cached JSON responses into a dataframe.
        frames = [
            DataFrame(result['response']['json']['data'], columns=self.schema.keys())
            for cache in caches
            for result in cache.load()
        ]
        df = pandas.concat(frames, ignore_index=True)

        # Log, return.
        log.debug(f'Done with endpoint = {endpoint}, filters = {filters}, min_date = {min_date}, max_date = {max_date}, caches = {len(caches)}, rows = {len(df):,}.')
        return df

    def _get_cache_prefix(self, endpoint: str, filters: dict) -> Path:
        """Returns cache path prefix for given endpoint and filter set.  (Organizes disk storage.)"""
        min_score = filters.get('min_score') if filters.get('min_score') else 'null'
        suffix = ', '.join(f'{k}={v}' for k, v in filters.items() if k != 'min_score')
        return paths.data / f'reddit_{endpoint}s' / f'min_score={min_score}' / suffix

    def _get_cache_key(self, endpoint: str, filters: dict) -> str:
        """Returns cache dict key for given endpoint and filter set.  (Organizes in-memory storage.)"""
        return json.dumps({'endpoint': endpoint, 'filters': filters}, sort_keys=True)

import logging
import pandas as pd
import requests
from datetime import datetime
from pandas import DataFrame
from typing import Dict, List
log = logging.getLogger(__name__)



def load_comments(q: str, start_date: datetime, end_date: datetime, max_iterations: int = 3600) -> List[Dict]:
    """
    Iteratively queries the Pushshift API, and returns a list all comments posted between
    `start_date` and `end_date` mentioning the word `q`.

    References:
        Pushshift API:
        https://github.com/pushshift/api

    TODO:
        Add metadata to results, e.g. iteration number, request time, request params, etc.
        Add caching logic.
        Add dataframe conversion logic (handle empty responses).
    """

    log.debug('Begin load_comments.')
    log.debug(f'q = {q}.')
    log.debug(f'start_date = {start_date}.')
    log.debug(f'end_date = {end_date}.')
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
        request_time = datetime.now()
        response = requests.get(url='https://api.pushshift.io/reddit/search/comment', params=params)
        batch = response.json()['data']

        # If batch is empty, our query is complete.
        # TODO:  Return empty dataframe with proper columns?
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
        results.extend(batch)
        log.debug(f'i = {i}, total = {len(results):,}, batch = {len(batch):,}, batch_min = {batch_min_date}, batch_max = {batch_max_date}, estimated = {estimated_iterations:.2f}.')
        i += 1
        batch_min_date = batch_max_date

        # If maximum number iterations exceeded, stop early.
        if i == max_iterations - 1:
            log.warning(f'i = {i}, max iterations exceeded.')
            return results

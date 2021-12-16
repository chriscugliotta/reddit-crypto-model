import requests
from datetime import datetime
from cc_idea.utils.retry_utils import retry_with_timeout



@retry_with_timeout(tries=5, delay=5, timeout=20)
def get_request(url: str, params: dict, iteration: int = 0) -> dict:
    """
    Wraps `requests.get` call for multiple reasons:

        1.  We add retry logic, to ensure network blips and throttled requests don't kill the app.

        2.  We wrap the JSON response with some metadata, such as the `iteration` (in case we need
            to troubleshoot an iterative pull, post-mortem).

        3.  We ensure the response is JSON-serializable (so that it can be cached).
    """
    request_time = datetime.utcnow()
    response = requests.get(url=url, params=params)
    response.raise_for_status()
    response_json = response.json()
    return {
        'request': {
            'time': request_time.timestamp(),
            'url': url,
            'params': params,
            'iteration': iteration,
        },
        'response': {
            'elapsed': response.elapsed.total_seconds(),
            'reason': response.reason,
            'status_code': response.status_code,
            'json': response_json,
            'rows': len(response_json.get('data', [])),
        }
    }

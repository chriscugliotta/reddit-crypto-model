import logging
import time
from functools import wraps
from multiprocessing.pool import ThreadPool
from typing import Any
log = logging.getLogger(__name__)



def retry_with_timeout(tries: int, delay: int, timeout: int) -> Any:
    """
    Calls a function in a separate thread with retries and a timeout condition.

    Args:
        tries (int):
            The maximum number of times we will attempt the wrapped function.

        delay (int):
            Delay in seconds between failed attempts.

        timeout (int):
            Timeout in seconds.  If the function's runtime exceeds this amount, a timeout exception
            is raised, which triggers a retry.  (Be careful, the original function/thread is not
            killed.)

    Returns:
        Any:  The function's return value.

    References:
        This implementation was inspired by:
        https://stackoverflow.com/questions/29494001/how-can-i-abort-a-task-in-a-multiprocessing-pool-after-a-timeout
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):

            for i in range(tries):
                try:
                    with ThreadPool(1) as pool:
                        apply_result = pool.apply_async(func, args=args, kwds=kwargs)
                        return apply_result.get(timeout=timeout)
                except Exception as e:
                    log.warning(f'Attempt {i + 1} of {tries} failed with {e.__class__.__name__}: {e}.')
                    if i + 1 < tries:
                        time.sleep(delay)
                    else:
                        raise e

        return wrapper
    return decorator

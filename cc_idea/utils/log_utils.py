import logging
import logging.config
from pathlib import Path



def initialize_logger(log_path: Path = None):
    """
    Configures the Python logger.
    Log messages will be sent to console, and optionally to a file as well.

    Args:
        log_path (Path or str):
            Log file path.  If omitted, no log file will be generated.
    """

    config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'class': 'logging.Formatter',
                'format': '%(asctime)s %(levelname)-8s %(name)-25s %(funcName)-25s %(message)s',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'standard',
            },
            'file': {
                'class': 'logging.FileHandler',
                'formatter': 'standard',
                'filename': log_path,
                'mode': 'w',
            },
        },
        'loggers': {
            'cc_idea': {
                'level': 'DEBUG',
                'handlers': ['console', 'file'],
            },
        },
    }

    if log_path is None:
        del config['handlers']['file']
        del config['loggers']['cc_idea']['handlers'][1]

    logging.config.dictConfig(config)

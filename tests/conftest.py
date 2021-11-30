# External imports.
import logging
import pytest
import sys
from pathlib import Path

# Hack Python path.
path_repo = Path(__file__).parents[1]
if str(path_repo) not in sys.path:
    sys.path.insert(0, str(path_repo))

# Internal imports.
from cc_idea.core.config import paths
from cc_idea.utils.log_utils import initialize_logger

# Logger.
log = logging.getLogger(__name__)



@pytest.fixture(scope='session', autouse=True)
def log_file(worker_id):
    log_path = paths.tests / 'logs' / 'log_{}.log'.format(_get_worker_suffix(worker_id))
    log_path.parent.mkdir(parents=True, exist_ok=True)
    initialize_logger(log_path)
    log.info('worker_id = {0}'.format(worker_id))


def _get_worker_suffix(worker_id):
    return '0' if worker_id == 'master' else worker_id[-1]

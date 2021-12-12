import logging
from datetime import datetime
from cc_idea.core.config import paths
from cc_idea.extractors.reddit import get_comments
from cc_idea.utils.log_utils import initialize_logger
log = logging.getLogger('cc_idea')



if __name__ == '__main__':

    initialize_logger(paths.repo / 'log.log')
    log.info('Begin.')

    df = get_comments(
        q='cardano',
        start_date=datetime(2021, 1, 1, 0, 0, 0),
        end_date=datetime(2021, 1, 2, 0, 0, 0),
    )

    log.info('Done.')

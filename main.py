import logging
from datetime import datetime
from cc_idea.loaders.reddit import load_comments
from cc_idea.utils.log_utils import initialize_logger
log = logging.getLogger('cc_idea')



if __name__ == '__main__':

    initialize_logger()
    log.info('Begin.')

    df = load_comments(
        q='cardano',
        start_date=datetime(2021, 1, 1, 0, 0, 0),
        end_date=datetime(2021, 1, 1, 16, 0, 0),
    )

    log.info('Done.')

"""Storage methods and utilities"""

import os
import re
import shutil
import time

from taca.log import LOG
from taca.utils import filesystem

def cleanup(config, days, run=None):
    for data_dir in config.get('storage').get('data_dirs'):
        with filesystem.chdir(data_dir):
            for run in [r for r in os.listdir(data_dir) if re.match(filesystem.RUN_RE, r)]:
                rta_file = os.path.join(run, 'RTAComplete.txt')
                if os.path.exists(rta_file):
                    # 1 day == 60*60*24 seconds --> 86400
                    if os.stat(rta_file).st_mtime < time.time() - (86400 * days):
                        LOG.info('Moving run {} to nosync directory'.format(os.path.basename(run)))
                        shutil.move(run, 'nosync')
                    else:
                        LOG.info('RTAComplete.txt file exists but is not older than {} day(s), skipping run {}'.format(str(days), run))

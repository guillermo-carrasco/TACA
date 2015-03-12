"""Storage methods and utilities"""

import os
import re
import time
import shutil

from multiprocessing import Pool

from taca.log import get_logger
from taca.utils.config import get_config
from taca.utils import filesystem, misc
from datetime import datetime
from statusdb.db import connections as statusdb

## default thershold days to check on different sites ##
site_check_days = {'nas' : 2, 'swestore' : 480, 'proc' : 2,
                   'archive' : 90, 'illumina' : 90, 'analysis' : 90}

def cleanup_nas(days):
    """
        Will move the finished runs in NASes and processing server
        to nosync directory, so they will not be processed anymore
    """
    config = get_config()
    LOG = get_logger()
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


def archive_to_swestore(days, run=None):
    """
        Send runs (as archives) in NAS nosync to swestore for backup
        
        :param int days: number fo days to check threshold
        :paran str run: specific run to send swestore
    """
    config = get_config()
    LOG = get_logger()
    # If the run is specified in the command line, check that exists and archive
    if run:
        run = os.path.basename(run)
        base_dir = os.path.dirname(run)
        if re.match(filesystem.RUN_RE, run):
            # If the parameter is not an absolute path, find the run in the archive_dirs
            if not base_dir:
                for archive_dir in config.get('storage').get('archive_dirs'):
                    if os.path.exists(os.path.join(archive_dir, run)):
                        base_dir = archive_dir
            if not os.path.exists(os.path.join(base_dir, run)):
                LOG.error(("Run {} not found. Please make sure to specify "
                    "the absolute path or relative path being in the correct directory.".format(run)))
            else:
                with filesystem.chdir(base_dir):
                    _archive_run(run)
        else:
            LOG.error("The name {} doesn't look like an Illumina run".format(os.path.basename(run)))
    # Otherwise find all runs in every data dir on the nosync partition
    else:
        LOG.info("Archiving old runs to SWESTORE")
        for to_send_dir in config.get('storage').get('archive_dirs'):
            LOG.info('Checking {} directory'.format(to_send_dir))
            with filesystem.chdir(to_send_dir):
                to_be_archived = [r for r in os.listdir(to_send_dir) if re.match(filesystem.RUN_RE, r)
                                            and not os.path.exists("{}.archiving".format(r.split('.')[0]))]
                if to_be_archived:
                    pool = Pool(processes=len(to_be_archived))
                    pool.map_async(_archive_run, ((run,) for run in to_be_archived))
                    pool.close()
                    pool.join()
                else:
                    LOG.info('No old runs to be archived')


def cleanup_swestore(days,dry_run=False):
    """ 
        Remove archived runs from swestore
        
        :param int days: Threshold days to check and remove
    """
    config = get_config()
    LOG = get_logger()
    runs = filesystem.list_runs_in_swestore(path=config.get('cleanup').get('swestore'))
    for run in runs:
        date = run.split('_')[0]
        if misc.days_old(date) > days:
            if dry_run:
                LOG.info('Will remove file {} from swestore'.format(run))
                continue
#            misc.call_external_command('irm -f {}'.format(run))
            LOG.info('Removed file {} from swestore'.format(run))


def cleanup_project(site,days,dry_run=False):
    """
        Remove project that have been colsed more than 'days'
        from the given 'site'
        
        :param str site: site where the cleanup should be performed
        :param int days: number of days to check for closed projects
    """
    config = get_config()
    LOG = get_logger()
    delete_log = "delivered_and_deleted"
    PRO_RE = '[a-zA-Z]+\.[a-zA-Z]+_\d{2}_\d{2}'
    root_dir = config.get('cleanup').get(site)
    db_config = config.get('statusdb',{})
    # make a connection for project db #
    pcon = statusdb.ProjectSummaryConnection(**db_config)
    assert pcon, "Could not connect to project database in StatusDB"
    with filesystem.chdir(root_dir):
        projects = [ p for p in os.listdir(root_dir) if re.match(PRO_RE,p) ]
        for proj in projects:
            if proj not in pcon.name_view.keys():
                LOG.warn("Project {} is not in database, so SKIPPING it..".format(proj))
                continue
            proj_db_obj = pcon.get_entry(proj)
            proj_close_date = proj_db_obj.get('close_date')
            if proj_close_date and misc.days_old(proj_close_date,date_format='%Y-%m-%d') > days:
                if dry_run:
                    LOG.info('Will remove project {} from {}'.format(proj,root_dir))
                    continue
                remove_and_log_path(path=proj, log_file='{fl}/{fl}.log'.format(fl=delete_log), logger=LOG)
                LOG.info('Removed project {} from {}'.format(proj,root_dir))
            else:
                LOG.warn("Project {} is either open or too old or closed within {} days, so SKIPPING it..".format(proj,days))
            

#############################################################
# Class helper methods, not exposed as commands/subcommands #
#############################################################
def _archive_run((run,)):
    """ 
        Archive a specific run to swestore
        
        :param str run: Run directory
    """
    config = get_config()
    LOG = get_logger()

    def _send_to_swestore(f, dest, remove=True):
        """ Send file to swestore checking adler32 on destination and eventually
        removing the file from disk

        :param str f: File to remove
        :param str dest: Destination directory in Swestore
        :param bool remove: If True, remove original file from source
        """
        if not filesystem.is_in_swestore(f):
            LOG.info("Sending {} to swestore".format(f))
            misc.call_external_command('iput -K -P {file} {dest}'.format(file=f, dest=dest),
                    with_log_files=True)
            LOG.info('Run {} sent correctly and checksum was okay.'.format(f))
        else:
            LOG.warn('Run {} is already in Swestore, not sending it again'.format(f))
        if remove and filesystem.is_in_swestore(run):
            LOG.info('Removing run'.format(f))
            os.remove(f)
        os.remove("{}.archiving".format(f.split('.')[0]))

    # Create state file to say that the run is being archived
    open("{}.archiving".format(run.split('.')[0]), 'w').close()
    if run.endswith('bz2'):
        _send_to_swestore(run, config.get('storage').get('irods').get('irodsHome'))
    else:
        LOG.info("Compressing run {}".format(run))
        # Compress with pbzip2
        misc.call_external_command('tar --use-compress-program=pbzip2 -cf {run}.tar.bz2 {run}'.format(run=run))
        LOG.info('Run {} successfully compressed! Removing from disk...'.format(run))
        shutil.rmtree(run)
        _send_to_swestore('{}.tar.bz2'.format(run), config.get('storage').get('irods').get('irodsHome'))


def remove_and_log_path(path,log_file,logger=None):
    """
        Will delete the path and log info on log_file
        
        :param str path: the path to be removed
        :param str log_file: a path to log the delete records
    """
    assert os.path.exists(path), "Path {} does not exist in {}".format(path,os.getcwd())
    assert os.path.exists(os.path.dirname(log_file)), "Log file path {} doesn't exist".format(log_file)
    mode = 'a' if os.path.exists(log_file) else 'w'
    try:
#        shutil.rmtree(path)
        with open(log_file,mode) as to_log:
            t = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M')
            to_log.write("{}\t{}\n".format(path,t))
    except OSError:
        if logger:
            logger.warn("Could not remove path {} from {}".format(path,os.getcwd()))
        pass
        
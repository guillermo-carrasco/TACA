""" TACA controllers
"""
import os
import re
import shutil
import time

from cement.core import controller

from taca.controllers import BaseController
from taca.utils import filesystem, misc

class StorageController(BaseController):
    """ Storage Controller

    Entry point for all functionalities related to storage
    """
    class Meta:
        label = 'storage'
        description = "Entry point for all functionalities related to storage"
        stacked_on = 'base'
        stacked_type = 'nested'
        arguments = [
            (['-r', '--run'], dict(type=str, help="Work with a specific run")),
            (['-d', '--days'], dict(type=int, default=10, help="Days to consider a run \"old\""))
        ]

    #######################
    # Storage subcommands #
    #######################

    @controller.expose(help="Move old runs to nosync directory so they're not synced to the processing server")
    def cleanup(self):
        for data_dir in self.app.config.get('storage', 'data_dirs'):
            with filesystem.chdir(data_dir):
                for run in [r for r in os.listdir(data_dir) if re.match(filesystem.RUN_RE, r)]:
                    rta_file = os.path.join(run, 'RTAComplete.txt')
                    if os.path.exists(rta_file):
                        # 1 day == 60*60*24 seconds --> 86400
                        if os.stat(rta_file).st_mtime < time.time() - 86400:
                            self.app.log.info('Moving run {} to nosync directory'.format(os.path.basename(run)))
                            shutil.move(run, 'nosync')
                        else:
                            self.app.log.info('RTAComplete.txt file exists but is not older than 1 day, skipping run {}'.format(run))


    @controller.expose(help="Archive old runs to SWESTORE")
    def archive_to_swestore(self):
        # If the run is specified in the command line, check that exists and archive
        if self.app.pargs.run:
            run = os.path.basename(self.app.pargs.run)
            base_dir = os.path.dirname(self.app.pargs.run)
            if re.match(filesystem.RUN_RE, run):
                # If the parameter is not an absolute path, find the in the archive_dirs
                if not base_dir:
                    for archive_dir in self.app.config.get('storage', 'archive_dirs'):
                        if os.path.exists(os.path.join(archive_dir, run)):
                            base_dir = archive_dir
                if not os.path.exists(os.path.join(base_dir, run)):
                    self.app.log.error(("Run {} not found. Please make sure to specify "
                        "the absolute path or relative path being in the correct directory.".format(run)))
                else:
                    with filesystem.chdir(base_dir):
                        self._archive_run(run)
            else:
                self.app.log.error("The name {} doesn't look like an Illumina run".format(os.path.basename(run)))
        # Otherwise find all runs in every data dir on the nosync partition
        else:
            self.app.log.info("Archiving old runs to SWESTORE")
            for data_dir in self.app.config.get('storage', 'data_dirs'):
                to_send_dir = os.path.join(data_dir, 'nosync')
                self.app.log.info('Checking {} directory'.format(to_send_dir))
                with filesystem.chdir(to_send_dir):
                    for run in [r for r in os.listdir(to_send_dir) if re.match(filesystem.RUN_RE, r)]:
                        self._archive_run(run)

    #############################################################
    # Class helper methods, not exposed as commands/subcommands #
    #############################################################
    def _archive_run(self, run):
        """ Archive a specific run to swestore

        :param str run: Run directory
        """
        def _send_to_swestore(f, dest, remove=True):
            """ Send file to swestore checking adler32 on destination and eventually
            removing the file from disk

            :param str f: File to remove
            :param str dest: Destination directory in Swestore
            :param bool remove: If True, remove original file from source
            """
            if not filesystem.is_in_swestore(f):
                self.app.log.info("Sending {} to swestore".format(f))
                misc.call_external_command('iput -K -P {file} {dest}'.format(file=f, dest=dest),
                        with_log_files=True)
                self.app.log.info('Run {} sent correctly and checksum was okay.'.format(f))
            else:
                self.app.log.warn('Run {} is already in Swestore, not sending it again'.format(f))
            if remove:
                self.app.log.info('Removing run'.format(f))
                os.remove(f)


        if run.endswith('bz2'):
            _send_to_swestore(run, self.app.config.get('storage', 'irods').get('irodsHome'))
        else:
            self.app.log.info("Compressing run {}".format(run))
            # Compress with pbzip2
            misc.call_external_command('tar --use-compress-program=pbzip2 -cf {run}.tar.bz2 {run}'.format(run=run))
            self.app.log.info('Run {} successfully compressed! Removing from disk...'.format(run))
            shutil.rmtree(run)
            _send_to_swestore('{}.tar.bz2'.format(run), self.app.config.get('storage', 'irods').get('irodsHome'))

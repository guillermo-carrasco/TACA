""" PM controllers
"""
import os
import re
import shutil

from cement.core import controller

from pm.controllers import BaseController
from pm.utils import filesystem, misc

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
                    if os.path.exists(os.path.join(run, 'RTAComplete.txt')):
                        self.app.log.info('Moving run {} to nosync directory'.format(os.path.basename(run)))
                        shutil.move(run, 'nosync')


    @controller.expose(help="Archive old runs to SWESTORE")
    def archive_to_swestore(self):
        # If the run is specified in the command line, check that exists and archive
        if self.app.pargs.run:
            if re.match(filesystem.RUN_RE, os.path.basename(self.app.pargs.run)):
                if not os.path.exists(self.app.pargs.run):
                    self.app.log.error(("Run {} not found. Please make sure to specify "
                        "the absolute path or relative path being in the correct directory.".format(self.app.pargs.run)))
                else:
                    self._archive_run(self.pargs.run)
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
            self.app.log.info("Sending {} to swestore".format(f))
            misc.call_external_command('iput -K -P {file} {dest}'.format(file=f, dest=dest),
                    with_log_files=True)
            # XXX Check adler32
            self.app.log.info('Run {} sent correctly and double-check was okay.'.format(f))
            if remove:
                self.app.log.info('Removing run'.format(f))
                shutil.rmtree(run)


        if run.endswith('bz2'):
            _send_to_swestore(run, self.app.config.get('storage', 'irods').get('irodsHome'))
        else:
            self.app.log.info("Compressing run {}".format(run))
            # Compress with pbzip2
            misc.call_external_command('tar --use-compress-program=pbzip2 -cf {run}.tar.bz2 {run}'.format(run=run))
            self.app.log.info('Run {} successfully compressed! Removing from disk...'.format(run))
            shutil.rmtree(run)
            _send_to_swestore('{}.tar.bz2'.format(run), self.app.config.get('storage', 'irods').get('irodsHome'))

""" PM controllers
"""
import os
import re

from cement.core import controller

from pm.controllers import BaseController
from pm.utils import filesystem

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

    @controller.expose(help="Clean old runs from the filesystem")
    def cleanup(self):
        raise NotImplementedError('To be implemented...')


    @controller.expose(help="Archive old runs to SWESTORE")
    def archive_to_swestore(self):
        # If the run is specified in the command line, check that exists and archive
        if self.app.pargs.run:
            if re.match(filesystem.RUN_RE, os.path.basename(run)):
                if not os.path.exists(self.app.pargs.run):
                    self.app.log.error(("Run {} not found. Please make sure to specify "
                        "the absolute path or relative path being in the correct directory.".format(self.app.pargs.run)))
                else:
                    self._archive_run(self.pargs.run)
            else:
                self.app.log.error("The {} doesn't look like an Illumina run".format(os.path.basename(run)))
        # Otherwise find all runs in every data dir
        else:
            self.app.log.info("Archiving old runs to SWESTORE")
            for data_dir in self.app.config.get('storage', 'data_dirs'):
                self.app.log.info('Checking {} directory'.format(data_dir))
                for run in [r for r in os.listdir(data_dir) if re.match(filesystem.RUN_RE, r)]:
                    self._archive_run(run)

    #############################################################
    # Class helper methods, not exposed as commands/subcommands #
    #############################################################
    def _archive_run(self, run):
        """ Archive a specific run to swestore

        :param str run: Run directory
        """
        if run.endswith('bz2'):
            self.app.log.info("Tarball")
            # XXX Check that md5sum exists, otherwise create it
            # XXX send tarball to swestore and check md5 and adler32
            pass
        else:
            self.app.log.info("Raw data")
            # Compress with pbzip2
            # Calculate md5sum
            # Send to swestore
            pass
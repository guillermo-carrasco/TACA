""" PM controllers
"""
import os 

from cement.core import controller

from pm.controllers import BaseController

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



    @controller.expose(help="Clean old runs from the filesystem")
    def cleanup(self):
        raise NotImplementedError('To be implemented...')


    @controller.expose(help="Archive old runs to SWESTORE")
    def archive_to_swestore(self):
        if self.app.pargs.run:
            if not os.path.exists(self.app.pargs.run):
                self.app.log.error(("Run {} not found. Please make sure to specify "
                    "the absolute path or relative path being in the correct directory.".format(self.app.pargs.run)))
            else:
                self.app.log.info('Archiving run ')
        else:
            self.app.log.info("Archiving old runs to SWESTORE")
            for d in self.app.config.get('storage', 'data_dirs'):
                self.app.log.info('Checking {} directory'.format(d))
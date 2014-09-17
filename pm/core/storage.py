""" Controllers and methods related with storage
"""
from cement.core import controller

from pm.core import BaseController

class StorageController(BaseController):
    """ Storage Controller

    Entry point for all functionalities related to storage
    """
    class Meta:
        label = 'storage'
        description = "Entry point for all functionalities related to storage"
        stacked_on = 'base'
        stacked_type = 'nested'


    @controller.expose(help="Cleans old runs from the filesystem")
    def cleanup(self):
        raise NotImplementedError('To be implemented...')
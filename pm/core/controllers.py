""" PM controllers
"""
from cement.core import controller

from pm.log import loggers

LOG = loggers.minimal_logger('PM Controllers')

hello_message = "Welcome to Project Management tools!"

class BaseController(controller.CementBaseController):
    """ Define an application BaseController

    The most basic controller. To be used as a template for new and more complex
    controllers.
    """
    class Meta:
        label = 'base'
        description = hello_message


    @controller.expose(hide=True)
    def default(self):
        print "Execute pm --help to display available commands"

    @controller.expose(hide=True, help="Prints a hello message")
    def hello(self):
        """ Testing method that just prints a hello message.

        Will not be listed as an available option (--hide)
        """
        self.app.log.info(hello_message)


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
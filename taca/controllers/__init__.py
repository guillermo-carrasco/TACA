""" Core module.

Place for controllers and other structural stuff.
"""
from cement.core import controller

class BaseController(controller.CementBaseController):
    """ Define an application BaseController

    The most basic controller. To be used as a template for new and more complex
    controllers.
    """
    class Meta:
        label = 'base'
        description = "Project Management - A tool for miscellaneous tasks at NGI"


    @controller.expose(hide=True)
    def default(self):
        print "Execute taca --help to display available commands"

    @controller.expose(hide=True, help="Prints a hello message")
    def hello(self):
        """ Testing method that just prints a hello message.

        Will not be listed as an available option (--hide)
        """
        self.app.log.info("Welcome to Project Management tools!")

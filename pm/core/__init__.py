from cement.core import controller

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
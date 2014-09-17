from cement.core import controller

hello_message = "Welcome to Project Management tools!"

# define an application base controller
class BaseController(controller.CementBaseController):
    class Meta:
        label = 'base'
        description = hello_message

    # This is for testing purposes. As it is hidden, won't show in --help
    @controller.expose(hide=True, help="Prints a hello message")
    def hello(self):
        self.app.log.info(hello_message)
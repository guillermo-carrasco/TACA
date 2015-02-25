How to add new code to TACA
=========================

TACA uses `cement`_ for building a structured CLI application. So for further details,
please refer to cement documentation.

Following there is a description of the basic steps you need to follow to add code to TACA.

Controllers
-----------
**NOTE:** *adding a controller requires modification to the main application script* ``taca``.

Assume you want to create a controller ``mycommand``. Create a file called ``mycommand.py``
and place it in ``taca/core/controllers``. Remember to create a doc string.


The ``BaseController`` is an interface, ensuring that all ``taca`` controllers behave similarly.
The minimum boilerplate code needed to define your new controller is:

.. code-block:: python

    ## Main mycommand controller
    class MycommandController(BaseController):
        """
        Functionality for mycommand.
        """
        class Meta:
            label = 'mycommand'
            description = 'Manage mycommand'
            stacked_on = 'base'
            stacked_type = 'nested'


        @controller.expose(hide=True)
        def default(self):
            pass

To add subcommands, add functions decorated with ``@controller.expose``:

.. code-block:: python

    @controller.expose(help="My subcommand")
    def mysubcommand(self):
        print "Mysubcommand"

That's all there is to it.

Before running, we need to modify the main application script. First you need to
import the newly defined command:

.. code-block:: python

    from taca.core.controllers import MycommandController

Then, before the application is setup (``app.setup()``) the command needs to be registered:

.. code-block:: python

    handler.register(MycommandController)

The new command can now be accessed as:

.. code-block:: bash

    taca mycommand
    taca mycommand mysubcommand

Remember that you can use the ``-h`` option any time, at any command level, to get
information about the available commands.

Subcontrollers
--------------

The main controllers are unstacked, *i.e.* their arguments are specific to each controller.
However, one can also add stacked controllers that add arguments to the main controllers.

To add a subcontroller to ``mycommand.py``, add:

.. code-block:: python

    class Mysubcommand2Controller(BaseController):
        class Meta:
            label = 'mysubcommand2-ctrl'
            stacked_on = 'mycommand'
            stacked_type = 'nested'
            description = 'Mysubcommand2 controller'
            arguments = [
                (['-f', '--foo'], dict(help="foo argument", default=False, action="store_true"))
            ]

    @controller.expose(help="Mysubcommand2 help")
    def mysubcommand2(self):
        print "mysubcommand2"

.. EXTERNAL LINKS

.. _cement: http://builtoncement.org/

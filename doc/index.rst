.. TACA documentation master file, created by
   sphinx-quickstart on Wed Sep 17 12:39:41 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. image:: _static/logo-alternative.png
    :width: 512
    :height: 175
    :align: center
    :alt: TACA logo

|

TACA stands for Tool for the Automation of Cleanup and Analyses, and basically it is a set of tools used in the
`National Genomics Infrastructure`_ for easing
the day-to-day tasks of managing and organizing projects and data.

To install TACA latest stable version, just use: ``pip install taca``, for the latest
development version, use ``pip install git+git://github.com/SciLifeLab/TACA.git``.

Once it is installed, to get help just use the ``--help`` option. You can use the
``--help`` option on every TACA subcommand to get specific command help. For example
``taca --help`` will give you

.. code-block:: bash

    Usage: taca [OPTIONS] COMMAND [ARGS]...

      Tool for the Automation of Storage and Analyses

    Options:
      --version                   Show the version and exit.
      -c, --config-file FILENAME  Path to TACA configuration file
      --help                      Show this message and exit.

    Commands:
      analysis  Analysis methods entry point
      storage   Storage management methods and utilities

And ``taca storage --help`` will give you

.. code-block:: bash

    Usage: taca storage [OPTIONS] COMMAND [ARGS]...

      Storage management methods and utilities

    Options:
      -d, --days INTEGER  Days to consider as thershold
      -r, --run PATH
      --help              Show this message and exit.

    Commands:
      archive  Archive old runs to SWESTORE
      cleanup  Do appropriate cleanup on the given site i.e.

Code and configuration
----------------------

.. toctree::
   :maxdepth: 2

   _templates/intro

API documentation
-----------------

.. toctree::
   :maxdepth: 3

   _templates/api/modules


.. EXTERNAL LINKS

.. _National Genomics Infrastructure: https://portal.scilifelab.se/genomics/

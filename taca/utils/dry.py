"""Scilifelab dry module

This module serves as a base to create methods that are able to be executed on dry
mode. This mode will just say what the method should do, but won't actually do anything.

To declare a task with the ability to be ran as dry, just pass it to the dry method
like this, for example:

def dry_makedir(dname, dry_run=True):
    def runpipe():
        if not os.path.exists(dname):
            try:
                os.makedirs(dname)
            except OSError:
                if not os.path.isdir(dname):
                    raise
        else:
            LOG.warn("Directory %s already exists" % dname)
        return dname
    return dry("Make directory %s" % dname, runpipe, dry_run)
"""
import os
import shutil
from cement.utils import shell

from taca.log import loggers

LOG = loggers.minimal_logger(__name__)

def dry(message, func, dry_run=True, *args, **kw):
    """Wrapper that runs a function (runpipe) if flag dry_run isn't set, otherwise returns function call as string

    :param str message: message describing function call
    :param func: function to call
    :param *args: positional arguments to pass to function
    :param **kw: keyword arguments to pass to function
    """
    if dry_run:
        LOG.debug("(DRY_RUN): " + str(message) + "\n")
        return "(DRY_RUN): " + str(message) + "\n"
    return func(*args, **kw)

""" Filesystem utilities
"""
import contextlib
import os

RUN_RE = '\d{6}_[a-zA-Z\d\-]+_\d{4}_[AB0][A-Z\d]'

@contextlib.contextmanager
def chdir(new_dir):
    """Context manager to temporarily change to a new directory.
    """
    cur_dir = os.getcwd()
    # This is weird behavior. I'm removing and and we'll see if anything breaks.
    #safe_makedir(new_dir)
    os.chdir(new_dir)
    try:
        yield
    finally:
        os.chdir(cur_dir)

""" Filesystem utilities
"""
import contextlib
import os

from subprocess import check_call, CalledProcessError

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


def is_in_swestore(f):
    """ Checks if a file exists in Swestore

    :param f str: File to check
    :returns bool: True if the file is already in Swestore, False otherwise
    """
    with open(os.devnull, 'w') as null:
        try:
            check_call(['ils', f], stdout=null, stderr=null)
        except CalledProcessError:
            # ils will fail if the file does not exist in
            return False
        else:
            return True

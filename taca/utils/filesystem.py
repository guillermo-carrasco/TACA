""" Filesystem utilities
"""
import contextlib
import os
import re
import shutil
from subprocess import check_call, CalledProcessError, Popen, PIPE

RUN_RE = '\d{6}_[a-zA-Z\d\-]+_\d{4}_[AB0][A-Z\d]'
PROJECT_RE = '[a-zA-Z]+\.[a-zA-Z]+_\d{2}_\d{2}'

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

def create_folder(target_folder):
    """ Ensure that a folder exists and create it if it doesn't, including any
        parent folders, as necessary.
        
        :param target_folder: the target folder 
        :returns: True if the folder exists or was created, False if the folder
        does not exists and could not be created
    """
    if not os.path.exists(target_folder):
        try:
            os.makedirs(target_folder)
        except OSError as e:
            return False
    return True

def is_in_swestore(f):
    """ Checks if a file exists in Swestore

    :param f str: File to check
    :returns bool: True if the file is already in Swestore, False otherwise
    """
    with open(os.devnull, 'w') as null:
        try:
            check_call(['ils', f], stdout=null, stderr=null)
        except CalledProcessError:
            # ils will fail if the file does not exist in swestore
            return False
        else:
            return True

def list_runs_in_swestore(path, pattern=RUN_RE, no_ext=False):
    """
        Will list runs that exist in swestore

        :param str path: swestore path to list runs
        :param str pattern: regex pattern for runs
    """
    try:
        status = check_call(['icd', path])
        proc = Popen(['ils'], stdout=PIPE)
        contents = [c.strip() for c in proc.stdout.readlines()]
        runs = [r for r in contents if re.match(pattern, r)]
        if no_ext:
            runs = [r.split('.')[0] for r in runs]
        return runs
    except CalledProcessError:
        return []


def is_in_file(file_path, text):
    """ Looks for text appearing in a file.

    :param str file_path: Path to the source file
    :param str text: Text to find in the file
    :raises OSError: If the file does not exist
    :returns bool: True is text is in file, False otherwise
    """
    with open(file_path, 'r') as f:
        content = f.read()
    return text in content

""" Miscellaneous or general-use methods
"""
from datetime import datetime
import subprocess
import sys

def call_external_command(cl, with_log_files=False):
    """ Executes an external command

    :param string cl: Command line to be executed (command + options and parameters)
    :param bool with_log_files: Create log files for stdout and stderr
    """
    stdout = sys.stdout
    stderr = sys.stderr

    if with_log_files:
        stdout = open(cl[0] + '.out', 'wa')
        stderr = open(cl[0] + '.err', 'wa')
        started = "Started command {} on {}".format(' '.join(cl), datetime.now())
        stdout.write(started + '\n')
        stdout.write('Command: {}\n'.format(' '.join(cl)))
        stdout.write(''.join(['=']*len(cl)) + '\n')

    try:
        subprocess.check_call(cl, stdout=stdout, stderr=stderr)
    except subprocess.CalledProcessError, e:
        e.message = "The command {} failed.".format(' '.join(cl))
        raise e
    finally:
        if with_log_files:
            stdout.close()
            stderr.close()
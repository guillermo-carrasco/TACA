""" Miscellaneous or general-use methods
"""
import os
import psutil
import subprocess
import sys

from datetime import datetime

def call_external_command(cl, with_log_files=False):
    """ Executes an external command

    :param string cl: Command line to be executed (command + options and parameters)
    :param bool with_log_files: Create log files for stdout and stderr
    """
    if type(cl) == str:
        cl = cl.split(' ')
    command = os.path.basename(cl[0])
    stdout = sys.stdout
    stderr = sys.stderr

    if with_log_files:
        stdout = open(command + '.out', 'wa')
        stderr = open(command + '.err', 'wa')
        started = "Started command {} on {}".format(' '.join(cl), datetime.now())
        stdout.write(started + '\n')
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



def call_external_command_detached(cl, with_log_files=False):
    """ Executes an external command

        :param string cl: Command line to be executed (command + options and parameters)
        :param bool with_log_files: Create log files for stdout and stderr
        """
    if type(cl) == str:
        cl = cl.split(' ')
    command = os.path.basename(cl[0])
    stdout = sys.stdout
    stderr = sys.stderr

    if with_log_files:
        stdout = open(command + '.out', 'wa')
        stderr = open(command + '.err', 'wa')
        started = "Started command {} on {}".format(' '.join(cl), datetime.now())
        stdout.write(started + '\n')
        stdout.write(''.join(['=']*len(cl)) + '\n')

    try:
        p_handle = subprocess.Popen(cl, stdout=stdout, stderr=stderr)
    except subprocess.CalledProcessError, e:
        e.message = "The command {} failed.".format(' '.join(cl))
        raise e
    finally:
        if with_log_files:
            stdout.close()
            stderr.close()
    return p_handle

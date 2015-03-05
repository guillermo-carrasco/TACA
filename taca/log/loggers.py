""" Logging module
"""
import logging
import os
import sys

from taca.utils import config as cl


def minimal_logger(namespace, config=None, to_file=True, debug=False):
    """Make and return a minimal console logger. Optionally write to a file as well.
    :param str namespace: Namespace of logger
    :param dict config: Dictionary with logging configurations
    :param bool to_file: Log to a file (location in configuration file)
    :return: A logging.Logger object
    :rtype: logging.Logger
    """
    log_level = logging.DEBUG if debug else logging.INFO
    log = logging.getLogger(namespace)
    log.setLevel(log_level)

    # Console logger
    s_h = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    s_h.setFormatter(formatter)
    s_h.setLevel(log_level)
    log.addHandler(s_h)

    # File logger
    if to_file:
        cwd = os.path.dirname(os.path.realpath('.'))
        log_path = os.path.join(os.environ['HOME'], 'taca.log')
        if config:
            log_path = config.get('log', {}).get('log_dir')
            if not log_path:
                raise RuntimeError("Section 'log' or option 'log_dir' were not found in the configuration file.")
        fh = logging.FileHandler(log_path)
        fh.setLevel(log_level)
        fh.setFormatter(formatter)
        log.addHandler(fh)
    return log

""" TACA logging module for external scripts
"""
import logging

LOG = logging.getLogger('TACA')
LOG.setLevel(logging.INFO)

# Console logger
s_h = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
s_h.setFormatter(formatter)
LOG.addHandler(s_h)

LOG_LEVELS = {
    'ERROR': logging.ERROR,
    'WARN': logging.WARN,
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG
}

def init_logger_file(log_file, log_level='INFO'):
    """ Append a FileHandler to the general logger.

    :param str log_file: Path to the log file
    :param str log_level: Logging level
    """
    log_level = LOG_LEVELS[log_level] if log_level in LOG_LEVELS.keys() else logging.INFO

    LOG.setLevel(log_level)

    fh = logging.FileHandler(log_file)
    fh.setLevel(log_level)
    fh.setFormatter(formatter)
    LOG.addHandler(fh)

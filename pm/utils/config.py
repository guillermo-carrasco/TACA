""" Load and parse configuration file
"""
import ConfigParser
import os

def load_config(config_file=None):
    """Loads a configuration file.

    By default it assumes ~/.pm/pm.conf
    """
    try:
        if not config_file:
            config_file = os.path.join(os.environ.get('HOME'), '.pm', 'pm.conf')
        config = ConfigParser.SafeConfigParser()
        with open(config_file) as f:
            config.readfp(f)
        return config
    except IOError:
        raise IOError(("There was a problem loading the configuration file. "
                "Please make sure that ~/.pm/pm.conf exists and that you have "
                "read permissions"))


def load_yaml_config(config_file_path):
    """Load YAML config file, expanding environmental variables.

    :param str config_file_path: The path to the configuration file.

    :returns: A dict of the parsed config file.
    :rtype: dict
    :raises IOError: If the config file cannot be opened.
    """
    return load_generic_config(config_file_path, config_format="yaml")


def load_generic_config(config_file_path, config_format="yaml", **kwargs):
    """Parse a configuration file, returning a dict. Supports yaml, xml, and json.

    :param str config_file_path: The path to the configuration file.
    :param str config_format: The format of the config file; default yaml.

    :returns: A dict of the configuration file with environment variables expanded.
    :rtype: dict
    :raises IOError: If the config file could not be opened.
    :raises ValueError: If config file could not be parsed.
    """
    parsers_dict = {"json": json.load,
                    "xml": xmltodict.parse,
                    "yaml": yaml.load,}
    try:
        parser_fn = parsers_dict[config_format.lower()]
    except KeyError:
        raise ValueError("Cannot parse config files in format specified "
                         "(\"{}\"): format not supported.".format(config_format))
    try:
        with open(config_file_path) as in_handle:
            config = parser_fn(in_handle, **kwargs)
            config = _expand_paths(config)
            return config
    except IOError as e:
        raise IOError("Could not open configuration file \"{}\".".format(config_file_path))
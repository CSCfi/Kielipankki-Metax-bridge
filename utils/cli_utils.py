import yaml


def config_from_file(config_file):
    """
    Read the YAML configuration from given file and return as a dict.

    If the configuration file is malformed or missing some mandatory values, an
    exception is raised.
    """
    try:
        config = yaml.load(config_file, Loader=yaml.BaseLoader)
    except yaml.YAMLError as e:
        raise ConfigurationError(
            "Given configuration file does not seem to be in YAML fromat: "
            f"{e}. See config/template.yml for valid configuration "
            "file example."
        )

    if type(config) != dict:
        raise ConfigurationError(
            "Unexpect configuration file structure. See config/template.yml for a "
            "valid configuration file example."
        )

    expected_configuration_values = [
        "metax_api_token",
        "metax_base_url",
        "metax_catalog_id",
        "harvester_log_file",
        "metax_api_log_file",
    ]

    for configuration_value in expected_configuration_values:
        if configuration_value not in config:
            raise ConfigurationError(
                f'Value for "{configuration_value}" not found in configuration file'
            )
    return config


class ConfigurationError(Exception):
    pass

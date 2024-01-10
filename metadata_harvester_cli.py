"""
Main script for running metadata harvesting and sending it to Metax.
"""

import logging
from datetime import datetime

import click
from lxml import etree
import yaml

from harvester.pmh_interface import PMH_API
from harvester.metadata_parser import MSRecordParser
from metax_api import MetaxAPI

logger_harvester = logging.getLogger("harvester")
logger_harvester.setLevel(logging.DEBUG)
file_handler_harvester = logging.FileHandler("harvester.log")
file_handler_harvester.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)
logger_harvester.addHandler(file_handler_harvester)


def last_harvest_date(log_file_path):
    """This function gets the start time of last successful harvesting date and time from the log
    if found.
    :param filename: string value of a file name
    :return: date and time
    """
    try:
        with open(log_file_path, "r") as log_file:
            lines = log_file.readlines()

            for i in range(len(lines) - 1, -1, -1):
                if "success" in lines[i].lower():
                    if i > 0 and "started" in lines[i - 1].lower():
                        log_datetime_str = lines[i - 1].split(" - ")[0]
                        log_datetime = datetime.strptime(
                            log_datetime_str, "%Y-%m-%d %H:%M:%S,%f"
                        )
                        return log_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
            return None
    except FileNotFoundError:
        return None


def _config_from_file(config_file):
    """
    Read the YAML configuration from given file and return as a dict.

    If the configuration file is malformed or missing some mandatory values, an
    exception is raised.
    """
    try:
        config = yaml.load(config_file, Loader=yaml.BaseLoader)
    except yaml.YAMLError as e:
        raise click.ClickException(
            "Given configuration file does not seem to be in YAML fromat: "
            f"{e}. See config/template.yml for valid configuration "
            "file example."
        )

    if type(config) != dict:
        raise click.ClickException(
            "Unexpect configuration file structure. See config/template.yml for a "
            "valid configuration file example."
        )

    if "metax_api_token" not in config:
        raise click.ClickException(
            'Value for "metax_api_token" not found in configuration file'
        )
    return config


@click.command()
@click.argument("log_file", type=click.Path(), default="harvester.log")
@click.argument("config_file", type=click.File("r"), default="config/config.yml")
def full_harvest(log_file, config_file):
    """
    Runs the whole pipeline of fetching data since last harvest and sending it to Metax.
    :param log_file: log file where harvest dates are logged
    """
    config = _config_from_file(config_file)
    metashare_api = PMH_API("https://kielipankki.fi/md_api/que")
    metax_api = MetaxAPI(api_token=config["metax_api_token"])

    harvested_date = last_harvest_date(log_file)
    logger_harvester.info("Started")

    for record in metashare_api.fetch_records(from_timestamp=harvested_date):
        metax_api.send_record(record)

    if harvested_date:
        logger_harvester.info("Success, records harvested since %s", harvested_date)
    else:
        logger_harvester.info("Success, all records harvested")

    metax_api.delete_records_not_in(metashare_api.fetch_records())


if __name__ == "__main__":
    full_harvest()  # pylint: disable=no-value-for-parameter

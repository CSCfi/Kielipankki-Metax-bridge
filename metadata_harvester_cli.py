"""
Main script for running metadata harvesting and sending it to Metax.
"""

import logging
from datetime import datetime
from requests.exceptions import (
    MissingSchema,
    InvalidSchema,
    InvalidURL,
    HTTPError,
    RequestException,
)
import traceback

import click
import yaml

from harvester.metadata_parser import RecordParsingError
from harvester.pmh_interface import PMH_API
from metax_api import MetaxAPI


def setup_cli_logger(log_file_name):
    logger_harvester = logging.getLogger("harvester")
    logger_harvester.setLevel(logging.DEBUG)
    file_handler_harvester = logging.FileHandler(log_file_name)
    file_handler_harvester.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    logger_harvester.addHandler(file_handler_harvester)

    return logger_harvester


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

    expected_configuration_values = [
        "metax_api_token",
        "metax_base_url",
        "metax_catalog_id",
        "harvester_log_file",
        "metax_api_log_file",
    ]

    for configuration_value in expected_configuration_values:
        if configuration_value not in config:
            raise click.ClickException(
                f'Value for "{configuration_value}" not found in configuration file'
            )
    return config


@click.command()
@click.argument("config_file", type=click.File("r"), default="config/config.yml")
def full_harvest(config_file):
    """
    Runs the whole pipeline of fetching data since last harvest and sending it to Metax.

    CONFIG_FILE Configuration for the harvesting. See config/template.yml for example.
    """
    config = _config_from_file(config_file)
    source_api = PMH_API("https://clarino.uib.no/oai")
    destination_api = MetaxAPI(
        base_url=config["metax_base_url"],
        catalog_id=config["metax_catalog_id"],
        api_token=config["metax_api_token"],
        api_request_log_path=config["metax_api_log_file"],
    )

    logger_harvester = setup_cli_logger(config["harvester_log_file"])
    harvested_date = last_harvest_date(config["harvester_log_file"])
    logger_harvester.info("Started")

    total_records = 0
    faulty_records = 0

    for record in source_api.fetch_corpora(from_timestamp=harvested_date):
        total_records += 1

        try:
            destination_api.send_record(record)
        except RecordParsingError as err:
            faulty_records += 1
            click.echo(err)
        except (MissingSchema, InvalidSchema, InvalidURL) as err:
            faulty_records += 1
            click.echo(
                f"There seems to be a configuration error related to Metax URL: {err}"
            )
            raise click.Abort()
        except HTTPError as err:
            faulty_records += 1
            click.echo(
                "HTTP request failed. "
                f"method: {err.request.method}, "
                f"URL: {err.request.url}, "
                f'error: "{err}", '
                f"response text: {err.response.text}, "
                f"payload: {err.request.body}"
            )
        except RequestException as err:
            faulty_records += 1
            click.echo(f"Error making a HTTP request: {err}")
        except:  # pylint: disable=bare-except
            faulty_records += 1
            click.echo(f"Unexpected problem with {record.pid}:")
            click.echo(traceback.format_exc())

    if not faulty_records:
        if harvested_date:
            logger_harvester.info(
                "Success, %d records harvested since %s", total_records, harvested_date
            )
        else:
            logger_harvester.info("Success, %d records harvested", total_records)
    else:
        if harvested_date:
            logger_harvester.info(
                "Success, %d records harvested since %s (out of which %d faulty "
                "record(s) not uploaded and will not be automatically retried)",
                total_records,
                harvested_date,
                faulty_records,
            )
        else:
            logger_harvester.info(
                "Success, %d records harvested (%d faulty record(s) not uploaded and will not "
                "be automatically retried)",
                total_records,
                faulty_records,
            )

    try:
        destination_api.delete_records_not_in(source_api.fetch_records())
    except RecordParsingError as err:
        click.echo(
            f"Error when determining records to be removed from Metax: {err}. Deletion of further "
            "records will not be attempted."
        )
        raise click.Abort()
    except HTTPError as err:
        click.echo(
            "Error deleting a record from Metax. Deletion of further records will not "
            "be attempted. "
            f"method: {err.request.method}, "
            f"URL: {err.request.url}, "
            f'error: "{err}", '
            f"response text: {err.response.text}, "
            f"payload: {err.request.body}"
        )
        raise click.Abort()
    except RequestException as err:
        click.echo(
            "Error deleting a record from Metax. Deletion of further records will not "
            f"be attempted: {err}"
        )
        raise click.Abort()
    except:  # pylint: disable=bare-except
        click.echo("Unexpected problem when deleting a record from Metax:")
        click.echo(traceback.format_exc())
        raise click.Abort()

    if faulty_records:
        exit(1)


if __name__ == "__main__":
    full_harvest()  # pylint: disable=no-value-for-parameter

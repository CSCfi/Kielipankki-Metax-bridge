"""
Main script for running metadata harvesting and sending it to Metax.
"""

from datetime import datetime
import logging
import traceback

import click
from requests.exceptions import (
    MissingSchema,
    InvalidSchema,
    InvalidURL,
    HTTPError,
    RequestException,
)
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
@click.option(
    "--pause-between-records",
    is_flag=True,
    default=False,
    help="Ask for confirmation before progressing to the next record",
)
@click.option(
    "--automatic-delete",
    is_flag=True,
    default=False,
    help="Automatically delete records found in destination API but not in source API",
)
def full_harvest(config_file, pause_between_records, automatic_delete):
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
        except RecordParsingError as error:
            faulty_records += 1
            click.echo(error, err=True)
        except (MissingSchema, InvalidSchema, InvalidURL) as error:
            faulty_records += 1
            click.echo(
                f"There seems to be a configuration error related to Metax URL: {error}",
                err=True,
            )
            raise click.Abort()
        except HTTPError as error:
            faulty_records += 1
            click.echo(
                "HTTP request failed. "
                f"method: {error.request.method}, "
                f"URL: {error.request.url}, "
                f'error: "{error}", '
                f"response text: {error.response.text}, "
                f"payload: {error.request.body}",
                err=True,
            )
        except RequestException as error:
            faulty_records += 1
            click.echo(f"Error making a HTTP request: {error}", err=True)
        except Exception:
            faulty_records += 1
            click.echo(f"Unexpected problem with {record.pid}:", err=True)
            click.echo(traceback.format_exc(), err=True)
            raise click.Abort()

        if pause_between_records:
            click.echo(f"Processed {record.pid}")
            selection = click.prompt(
                "Continue?",
                type=click.Choice(["next", "all", "abort"]),
                show_choices=True,
            )
            if selection == "abort":
                raise click.Abort()
            if selection == "all":
                pause_between_records = False

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

    if automatic_delete:
        delete_records(source_api, destination_api)
    else:
        pids_to_be_deleted = destination_api.pids_to_be_deleted(
            retained_records=source_api.fetch_records()
        )
        if pids_to_be_deleted:
            click.echo(
                "The records with following PIDs were not found in source data and should "
                "be deleted from Metax:",
                err=True,
            )
            for pid in pids_to_be_deleted:
                click.echo(f"- {pid}", err=True)

    if faulty_records:
        exit(1)


def delete_records(source_api, destination_api):
    try:
        destination_api.delete_records_not_in(source_api.fetch_records())
    except RecordParsingError as error:
        click.echo(
            f"Error when determining records to be removed from Metax: {error}. Deletion of further "
            "records will not be attempted.",
            err=True,
        )
        raise click.Abort()
    except HTTPError as error:
        click.echo(
            "Error deleting a record from Metax. Deletion of further records will not "
            "be attempted. "
            f"method: {error.request.method}, "
            f"URL: {error.request.url}, "
            f'error: "{error}", '
            f"response text: {error.response.text}, "
            f"payload: {error.request.body}",
            err=True,
        )
        raise click.Abort()
    except RequestException as error:
        click.echo(
            "Error deleting a record from Metax. Deletion of further records will not "
            f"be attempted: {error}",
            err=True,
        )
        raise click.Abort()
    except Exception:
        click.echo("Unexpected problem when deleting a record from Metax:", err=True)
        click.echo(traceback.format_exc(), err=True)
        raise click.Abort()


if __name__ == "__main__":
    full_harvest()  # pylint: disable=no-value-for-parameter

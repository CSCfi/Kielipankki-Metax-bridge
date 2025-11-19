import click
from requests.exceptions import (
    MissingSchema,
    InvalidSchema,
    InvalidURL,
    HTTPError,
    RequestException,
)

from harvester.metadata_parser import RecordParsingError
from harvester.pmh_interface import PMH_API
from utils import cli_utils
from metax_api import MetaxAPI


@click.group
def cli():
    """
    Helper functions for doing operations against Metax by hand
    """


@cli.command
@click.argument("config_file", type=click.File("r"), default="config/config.yml")
@click.argument("lb_pid", type=str)
def delete_record(config_file, lb_pid):
    """
    Delete a single record from Metax.

    \b
    CONFIG_FILE Configuration for the harvesting. See config/template.yml for
                example.
    LB_PID      Identifier of the record to be deleted, e.g.
                urn:nbn:fi:lb-1999010101
    """
    try:
        config = cli_utils.config_from_file(config_file)
    except cli_utils.ConfigurationError as err:
        raise click.ClickException(str(err))

    metax_api = MetaxAPI(
        base_url=config["metax_base_url"],
        catalog_id=config["metax_catalog_id"],
        api_token=config["metax_api_token"],
        api_request_log_path=config["metax_api_log_file"],
    )

    metax_id = metax_api.record_id(lb_pid)

    if not metax_id:
        print(f"Record {lb_pid} not found in Metax")
        return

    click.echo(f"Deleting record {lb_pid} (Metax identifier {metax_id}) from Metax")
    metax_api.delete_record(metax_id)
    click.echo("Record deleted")


@cli.command
@click.argument("config_file", type=click.File("r"), default="config/config.yml")
def incompatible_records_report(config_file):
    """
    List records with at least one Metax-incompatibility.

    If the same record has more than one problem, only the one encountered first will
    be reported. The output is formatted as tab-separated lines containing URN of the
    corpus, name of the corpus, and the detected incompatibility.

    NB: trying to upload records to Metax is a part of the validation process. Make sure
    that this is not a problem.

    \b
    CONFIG_FILE Configuration for the harvesting. See config/template.yml for
                example.
    """
    try:
        config = cli_utils.config_from_file(config_file)
    except cli_utils.ConfigurationError as err:
        raise click.ClickException(str(err))

    click.confirm(
        f"Records are about to be submitted to {config['metax_base_url']} during the "
        f"validation process. Do you want to proceed?",
        abort=True,
    )

    source_api = PMH_API("https://clarino.uib.no/oai")
    metax_api = MetaxAPI(
        base_url=config["metax_base_url"],
        catalog_id=config["metax_catalog_id"],
        api_token=config["metax_api_token"],
        api_request_log_path=config["metax_api_log_file"],
    )

    faulty_records = 0

    def _problematic_record_string(record, problem):
        try:
            # This is a bit ugly, but maximizes the likelihood of us getting the title
            # even if the metadata is otherwise problematic.
            # pylint: disable=protected-access
            title = record._get_element_text_in_preferred_language(
                "//cmd:resourceName"
            )["en"]
        except Exception:
            title = "-"

        return f"{record.pid}\t{title}\t{problem}"

    for record in source_api.fetch_corpora():

        try:
            metax_api.send_record(record)
        except RecordParsingError as error:
            faulty_records += 1
            click.echo(_problematic_record_string(record, str(error.message)))
        except (MissingSchema, InvalidSchema, InvalidURL) as error:
            faulty_records += 1
            click.echo(
                f"There seems to be a configuration error related to Metax URL: {error}",
                err=True,
            )
            raise click.Abort()
        except HTTPError as error:
            faulty_records += 1
            click.echo(_problematic_record_string(record, error.response.text))
        except RequestException as error:
            faulty_records += 1
            click.echo(f"Aborting: error making a HTTP request: {error}", err=True)
            raise click.Abort()
        except Exception:
            faulty_records += 1
            click.echo(f"Aborting: unexpected problem with {record.pid}:", err=True)
            click.echo(traceback.format_exc(), err=True)
            raise click.Abort()


if __name__ == "__main__":
    cli()  # pylint: disable=no-value-for-parameter

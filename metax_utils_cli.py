import click

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
    click.echo(f"Deleting record {lb_pid} (Metax identifier {metax_id}) from Metax")
    metax_api.delete_record(metax_id)
    click.echo("Record deleted")


if __name__ == "__main__":
    cli()  # pylint: disable=no-value-for-parameter

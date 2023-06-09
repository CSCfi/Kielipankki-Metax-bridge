"""
Command line interface for the metadata harvester
"""

import click

from harvester.pmh_interface import PMH_API


@click.command
@click.option(
    "--url", 
    default="https://kielipankki.fi/md_api/que", 
    help="URL of the OAI-PMH API"
    )
def retrieve_metadata_content(url):
    """
    Fetch all metadata records.
    """
    api = PMH_API(url)
    metadata_contents = api.get_all_metadata_records()
    for metadata_content in metadata_contents:
        click.echo(metadata_content)

if __name__ == "__main__":
    retrieve_metadata_content()
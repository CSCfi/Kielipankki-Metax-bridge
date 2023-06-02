"""
Command line interface for the metadata harvester
"""

import click

from harvester.pmh_interface import PMH_API


@click.command
@click.argument('url')
def retrieve_metadata_content(url):
    """
    Fetch all metadata records.
    """
    api = PMH_API(url)
    metadata_contents_list = api.get_all_metadata_records()
    record_count = len(metadata_contents_list)
    for metadata_content in metadata_contents_list:
        click.echo(metadata_content)
    click.echo(f"\nTotal Records: {record_count}\n")

if __name__ == "__main__":
    retrieve_metadata_content()
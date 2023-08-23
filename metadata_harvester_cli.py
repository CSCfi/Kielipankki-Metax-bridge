"""
Command line interface for the metadata harvester
"""

import click

from harvester.pmh_interface import PMH_API
from harvester.metadata_parser import MSRecordParser
from lxml import etree


@click.command
@click.option(
    "--url", 
    default="https://kielipankki.fi/md_api/que", 
    help="URL of the OAI-PMH API"
    )
def retrieve_metadata_content(url):
    """
    Fetch metadata records and transform them to JSON.
    """
    api = PMH_API(url)
    metadata_contents = api.get_all_metadata_records()
    for metadata_content in metadata_contents:
        lxml_record = etree.fromstring(etree.tostring(metadata_content.xml))
        metadata_record = MSRecordParser(lxml_record)
        click.echo(metadata_record.json_converter())

if __name__ == "__main__":
    retrieve_metadata_content()
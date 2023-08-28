"""
Command line interface for the metadata harvester
"""

from harvester.pmh_interface import PMH_API
from harvester.metadata_parser import MSRecordParser
import metax_api
from lxml import etree


def retrieve_metadata_content(url="https://kielipankki.fi/md_api/que"):
    """
    Fetch metadata records and transform them to JSON.
    """
    api = PMH_API(url)
    metadata_contents = api.get_all_metadata_records()
    for metadata_content in metadata_contents:
        lxml_record = etree.fromstring(etree.tostring(metadata_content.xml))
        metadata_record = MSRecordParser(lxml_record)
        

if __name__ == "__main__":
    retrieve_metadata_content()
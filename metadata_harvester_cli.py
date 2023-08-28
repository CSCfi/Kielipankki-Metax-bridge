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
    mapped_data_dict = {}
    metadata_contents = api.get_all_metadata_records()
    for metadata_content in metadata_contents:
        lxml_record = etree.fromstring(etree.tostring(metadata_content.xml))
        metadata_record = MSRecordParser(lxml_record)
        if metadata_record.check_pid_exists():
            if metadata_record.check_resourcetype_corpus():
                urn = metadata_record.get_identifier("//info:identificationInfo/info:identifier/text()")
                mapped_data_dict[urn] = metadata_record.json_converter()
    return mapped_data_dict


if __name__ == "__main__":
    retrieve_metadata_content()
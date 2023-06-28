import json
from lxml import etree


class MSRecordParser:
    def __init__(self, ms_record):
        """
        Create a Metashare record object.

        :param ms_record: a Metashare record
        """
        ms_xml_string = etree.tostring(ms_record.xml)
        self.ms_record_tree = etree.fromstring(ms_xml_string)

    def get_identifier(self):
        """
        Get the metadata identifier.
        """
        return self.ms_record_tree.xpath("//info:identificationInfo/info:identifier/text()", namespaces={'info': 'http://www.ilsp.gr/META-XMLSchema'})


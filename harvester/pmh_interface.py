"""
Fetch data from an OAI-PMH API of Metashare
"""

from lxml import etree
from sickle import Sickle
from sickle.oaiexceptions import NoRecordsMatch

from harvester.metadata_parser import MSRecordParser


class PMH_API:
    """
    Interface for fetching data from an OAI-PMH API
    """

    def __init__(self, url):
        """
        :param url: URL of the OAI-PMH API used
        """
        self.sickle = Sickle(url)

    def fetch_records(self, from_timestamp=None):
        """
        Fetch records that are new or updated since a date.

        If no date is given, all records are fetched.

        :param datetime: date (and time) string value
        """
        try:
            metadata_records = self.sickle.ListRecords(
                **{
                    "metadataPrefix": "info",
                    "from": from_timestamp,
                    "ignore_deleted": True,
                }
            )
            for metadata_record in metadata_records:
                yield metadata_record
        except NoRecordsMatch:
            return

    @property
    def corpus_pids(self):
        """
        PIDs for all corpora in Metashare.
        :return: List of PIDs as strings
        """
        metashare_pids = []
        for metadata_content in self.fetch_records():
            lxml_record = etree.fromstring(etree.tostring(metadata_content.xml))
            metadata_record = MSRecordParser(lxml_record)
            if (
                metadata_record.check_pid_exists()
                and metadata_record.check_resourcetype_corpus()
            ):
                pid = metadata_record.get_identifier(
                    "//info:identificationInfo/info:identifier/text()"
                )
                metashare_pids.append(pid)
        return metashare_pids

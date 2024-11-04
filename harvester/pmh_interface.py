"""
Fetch data from an OAI-PMH API
"""

from copy import deepcopy
from lxml import etree
from sickle import Sickle
from sickle.oaiexceptions import NoRecordsMatch

from harvester.metadata_parser import RecordParser


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

        :param from_timestamp: timestamp string defining the start of the period
        """
        try:
            metadata_records = self.sickle.ListRecords(
                **{
                    "metadataPrefix": "cmdi",
                    "from": from_timestamp,
                    "ignore_deleted": True,
                    "set": "FIN-CLARIN",
                }
            )
            for metadata_record in metadata_records:
                yield RecordParser(deepcopy(metadata_record.xml))
        except NoRecordsMatch:
            return

    def fetch_corpora(self, from_timestamp=None):
        """
        Iterate over all corpora type records that have a PID
        """
        for record in self.fetch_records(from_timestamp=from_timestamp):
            if record.check_resourcetype_corpus():
                yield record

    @property
    def corpus_pids(self):
        """
        PIDs for all corpora in the repository.

        :return: List of PIDs as strings
        """
        pids = []
        for corpus in self.fetch_corpora():
            pids.append(corpus.pid)
        return pids

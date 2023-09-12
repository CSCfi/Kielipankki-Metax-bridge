"""
Fetch data from an OAI-PMH API of Metashare
"""

from sickle import Sickle
from sickle.oaiexceptions import NoRecordsMatch

class PMH_API:
    """
    Interface for fetching data from an OAI-PMH API
    """

    def __init__(self, url):
        """
        :param url: URL of the OAI-PMH API used
        """
        self.sickle = Sickle(url)

    def fetch_records(self):
        """
        Fetch all non-deleted records from the API.
        """
        metadata_records = self.sickle.ListRecords(metadataPrefix="info", ignore_deleted=True)
        for metadata_record in metadata_records:
            yield metadata_record
    
    def fetch_changed_records(self, datetime):
        """
        Fetch records that are new or updated since a date.
        :param datetime: date (and time) string value
        """
        try:
            metadata_records = self.sickle.ListRecords(**{"metadataPrefix": "info","from": datetime,"ignore_deleted":True})
            for metadata_record in metadata_records:
                yield metadata_record
        except NoRecordsMatch:
            return None

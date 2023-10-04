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

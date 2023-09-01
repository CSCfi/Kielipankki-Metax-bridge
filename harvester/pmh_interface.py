"""
Fetch data from an OAI-PMH API of Metashare
"""

from sickle import Sickle


class PMH_API:
    """
    Interface for fetching data from an OAI-PMH API
    """

    def __init__(self, url):
        """
        :param url: URL of the OAI-PMH API used
        """
        self.sickle = Sickle(url)

    def get_all_metadata_records(self, limit=None):
        """
        Get content for all records available from the API.

        :param limit: Maximum number of records to retrieve. Default None fetches all records.
        """
        metadata_records = self.sickle.ListRecords(metadataPrefix="info", ignore_deleted=True)
        if not limit:
            yield from metadata_records
        else:
            for count, metadata_record in enumerate(metadata_records, start=1):
                yield metadata_record
                if count >= limit:
                    break
    def get_changed_records_from_last_harvest(self, date):
        """
        Fetch records that are new or updated since a date.
        """
        metadata_records = self.sickle.ListRecords(**{"metadataPrefix": "info","from": date,"ignore_deleted":True})
        for metadata_record in metadata_records:
            yield metadata_record
  

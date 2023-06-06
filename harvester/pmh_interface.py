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

    def get_all_metadata_records(self):
        """
        Get content for all records available from the API.
        """
        records = self.sickle.ListRecords(metadataPrefix="info")
        metadata_records_list = []
        for record in records:
            metadata_records_list.append(record)
        return metadata_records_list


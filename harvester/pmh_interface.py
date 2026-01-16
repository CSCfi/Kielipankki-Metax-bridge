"""
Fetch data from an OAI-PMH API
"""

from copy import deepcopy
from lxml import etree
from sickle import Sickle
from sickle.oaiexceptions import NoRecordsMatch

from harvester.metadata_parser import RecordParser, RecordParsingError


class PMH_API:
    """
    Interface for fetching data from an OAI-PMH API
    """

    def __init__(self, url):
        """
        :param url: URL of the OAI-PMH API used
        """
        self.sickle = Sickle(url)

    def fetch_records(self, from_timestamp=None, status="published"):
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
                    "status": status,
                }
            )
            for metadata_record in metadata_records:
                yield RecordParser(deepcopy(metadata_record.xml))
        except NoRecordsMatch:
            return

    def fetch_corpora(self, from_timestamp=None):
        """
        Iterate over all corpora type records that have a PID

        :raises [UknownResourceTypeError]: Raised when the resource type cannot be
            determined for one or more records. The iteration is completed before
            raising (to allow sending all non-problematic records to Metax) and all
            encountered errors are reported together using a single exception.
        """
        error_messages = []
        for record in self.fetch_records(from_timestamp=from_timestamp):
            try:
                if record.check_resourcetype_corpus():
                    yield record
            except RecordParsingError as err:
                error_messages.append(str(err))

        if error_messages:
            raise UnknownResourceTypeError(
                "Resource type could not be parsed for some record(s): "
                f"{'; '.join(error_messages)}",
                unknown_count=len(error_messages),
            )

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


class UnknownResourceTypeError(Exception):
    """
    Error raised when all corpora cannot be reliably iterated through
    """

    def __init__(self, message, unknown_count):
        super().__init__(message)
        self.message = message
        self.unknown_count = unknown_count

    def __str__(self):
        return f"Some records could not be categorized as corpora/other: {self.message}"

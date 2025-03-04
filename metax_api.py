import json
import logging
import requests
from requests.exceptions import HTTPError


class MetaxAPI:
    """
    An API client for interacting with the Metax V3 service.
    """

    def __init__(self, base_url, catalog_id, api_token, api_request_log_path):
        self.logger = self._setup_logger(api_request_log_path)
        self.base_url = base_url
        self.catalog_id = catalog_id
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Token {api_token}",
        }
        self.timeout = 30

    def _setup_logger(self, api_request_log_path):
        """
        Set up and configure a logger for logging Metax API requests and responses.
        """
        logger = logging.getLogger(api_request_log_path)
        logger.setLevel(logging.INFO)
        file_handler = logging.FileHandler(api_request_log_path)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        return logger

    def _make_request(self, method, endpoint, params=None, data=None):
        """
        Make an HTTP request to the specified endpoint.

        :param method: The HTTP method to use here: "GET", "POST", "PUT", or "DELETE".
        :param endpoint: The API endpoint to send the request to.
        :param params: A dictionary of query parameters to include in the request URL.
        :param data: A dictionary of data to include in the request body as JSON.

        :return: dict or None depending on if the request was successful.
        """
        url = f"{self.base_url}/{endpoint}"
        headers = self.headers

        try:
            response = requests.request(
                method,
                url,
                params=params,
                json=data,
                headers=headers,
                timeout=self.timeout,
            )
            response.raise_for_status()
            if method != "GET":
                self.logger.info("Request succeeded. Method: %s, URL: %s", method, url)
            if method == "DELETE":
                return response
            return response.json() if response.status_code == 200 else None
        except HTTPError as error:
            self.logger.error(
                "Request failed. Method: %s, URL: %s, Error: %s", method, url, error
            )
            raise

    def record_id(self, pid):
        """
        Get the UUID of a dataset record from Metax if such record exists.

        :param dataset pid: the persistent identifier of the record
        :return: the record identifier in Metax or None
        """
        params = {"data_catalog__id": self.catalog_id, "persistent_identifier": pid}
        endpoint = "datasets"
        result = self._make_request("GET", endpoint, params)
        return result["results"][0]["id"] if result["count"] == 1 else None

    def send_record(self, record):
        """
        Create or udpate a record on Metax.

        If a record with the same PID exists in Metax, its metadata is updated,
        otherwise a new record is created.

        :record: `MSRecordParser` object representing the metadata to be sent
        """
        metax_pid = self.record_id(record.pid)
        if metax_pid:
            self.update_record(metax_pid, record.to_dict(data_catalog=self.catalog_id))
        else:
            self.create_record(record.to_dict(data_catalog=self.catalog_id))

    def pids_to_be_deleted(self, retained_records):
        """
        Return a list of PIDs for records that should be deleted from Metax.

        Retained records are expected as an iterable of RecordParser instances.
        """
        retained_pids = {record.pid for record in retained_records}
        return self.datacatalog_record_pids.difference(retained_pids)

    def delete_records_not_in(self, retained_records):
        """
        Delete all records whose PIDs are not present in `retained_records`.

        This allows purging records from Metax when they have been marked as deleted in
        the master data. Deletion is determined based on whether each record in Metax
        has a counterpart with identical PID in `retained_records`.

        :retained_records: iterable containing all the records that must not be deleted.
        """
        for pid in self.pids_to_be_deleted(retained_records):
            self.delete_record(self.record_id(pid))

    def create_record(self, data):
        """
        Create a dataset record to Metax.
        :param metadata_dict: dictionary of metadata mappings
        :return: response JSON
        """
        endpoint = "datasets"
        return self._make_request("POST", endpoint, data=data)

    def update_record(self, record_id, data):
        """
        Update existing dataset record in Metax.
        :param metadata_dict: dictionary of metadata mappings
        :return: response JSON
        """
        endpoint = f"datasets/{record_id}"
        return self._make_request("PUT", endpoint, data=data)

    def delete_record(self, record_id):
        """
        Delete a dataset record from Metax.
        :param record_id: the record UUID in Metax
        """
        endpoint = f"datasets/{record_id}"
        return self._make_request("DELETE", endpoint)

    @property
    def datacatalog_record_pids(self):
        """
        Fetches all dataset record PIDs from catalog.

        :return: a set containing PIDs for all records in the dataset
        """
        url = f"{self.base_url}/datasets?data_catalog__id={self.catalog_id}&limit=100"
        results = []
        while url:
            response = requests.get(url, headers=self.headers, timeout=self.timeout)
            data = response.json()
            results.extend(data["results"])
            url = data["next"]

        return {value["persistent_identifier"] for value in results}

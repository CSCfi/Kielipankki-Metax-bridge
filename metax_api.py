import json
import logging
import requests
from requests.exceptions import HTTPError


class MetaxAPI:
    """
    An API client for interacting with the Metax V3 service.
    """

    def __init__(self):
        self.logger = self._setup_logger()
        self.base_url = "https://metax-service.fd-staging.csc.fi/v3"
        self.headers = {"Content-Type": "application/json"}
        self.catalog_id = "urn:nbn:fi:att:data-catalog-kielipankki"
        self.timeout = 30

    def _setup_logger(self):
        """
        Set up and configure a logger for logging Metax API requests and responses.
        """
        logger = logging.getLogger("metax_api_requests.log")
        logger.setLevel(logging.INFO)
        file_handler = logging.FileHandler("metax_api_requests.log")
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

    def datacatalog_record_pids(self):
        """
        Fetches all dataset record PIDs from catalog.
        :return: list of dataset record PIDs
        """
        url = f"{self.base_url}/datasets?data_catalog__id={self.catalog_id}&limit=100"
        results = []
        while url:
            response = requests.get(url, headers=self.headers, timeout=self.timeout)
            data = response.json()
            results.extend(data["results"])
            url = data["next"]
        return [value["persistent_identifier"] for value in results]

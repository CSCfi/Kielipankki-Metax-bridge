import json
import logging
import requests
from requests import HTTPError


class MetaxAPI:
    """
    An API client for interacting with the Metax V3 service.
    """
    METAX_BASE_URL = "https://metax-service.fd-staging.csc.fi/v3"
    HEADERS = {"Content-Type": "application/json"}
    CATALOG_ID = "urn:nbn:fi:att:data-catalog-kielipankki"
    TIMEOUT = 30

    def __init__(self):
        self.logger = self._setup_logger()

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
        url = f"{self.METAX_BASE_URL}/{endpoint}"
        headers = self.HEADERS

        try:
            response = requests.request(
                method,
                url,
                params=params,
                json=data,
                headers=headers,
                timeout=self.TIMEOUT,
            )
            response.raise_for_status()
            if method != "GET":
                self.logger.info(
                    "Request succeeded. Method: %s, URL: %s", method, url)
            if method == "DELETE":
                return None
            return response.json() if response.status_code == 200 else None
        except requests.exceptions.RequestException as error:
            self.logger.error(
                "Request failed. Method: %s, URL: %s, Error: %s", method, url, error
            )
            return None

    def record_id(self, pid):
        """
        Get the UUID of a dataset record from Metax if such record exists.
        :param dataset pid: the persistent identifier of the record
        :return: the record identifier in Metax or None
        """
        params = {"data_catalog__id": self.CATALOG_ID,
                  "persistent_identifier": pid}
        endpoint = "datasets"
        result = self._make_request("GET", endpoint, params)
        assert result is not None
        return result["results"][0]["id"] if result["count"] == 1 else None

    def create_record(self, data):
        """
        Create a dataset record to Metax.
        :param metadata_dict: dictionary of metadata mappings
        :return: response JSON
        """
        endpoint = "datasets"
        return self._make_request("POST", endpoint, data=data)

    def update_dataset(metax_dataset_id, metadata_dict):
        """
        Update existing dataset record in Metax.
        :param metadata_dict: dictionary of metadata mappings
        :return: the dataset identifier in Metax
        """
        response = requests.put(
            f"{METAX_BASE_URL}/datasets/{metax_dataset_id}",
            json=metadata_dict,
            headers=HEADERS,
            timeout=TIMEOUT,
        )
        try:
            response.raise_for_status()
        except HTTPError as error:
            logger_api.error(
                "Error: %s. Failed to update catalog record %s", error, metax_dataset_id
            )
            raise
        logger_api.info("Updated dataset. Response text: %s", response.text)
        return json.loads(response.text)["id"]

    def delete_dataset(metax_dataset_id):
        """
        Deletes a dataset record in Metax.
        :param metax_dataset_id: the dataset identifier in Metax
        :return: True
        """
        response = requests.delete(
            f"{METAX_BASE_URL}/datasets/{metax_dataset_id}",
            headers=HEADERS,
            timeout=TIMEOUT,
        )
        try:
            response.raise_for_status()
        except HTTPError as error:
            logger_api.error(
                "Error: %s. Failed to delete catalog record %s", error, metax_dataset_id
            )
            raise
        logger_api.info("Deleted dataset record %s", metax_dataset_id)
        return response

    def datacatalog_dataset_record_pids():
        """
        Fetches all dataset records from catalog.
        :return: list of dataset record PIDs
        """
        url = f"{METAX_BASE_URL}/datasets?data_catalog__id={KIELIPANKKI_CATALOG_ID}&limit=100"
        results = []
        while url:
            response = requests.get(
                url,
                headers=HEADERS,
                timeout=TIMEOUT)
            data = response.json()
            results.extend(data["results"])
            url = data["next"]
        return [value["persistent_identifier"] for value in results]

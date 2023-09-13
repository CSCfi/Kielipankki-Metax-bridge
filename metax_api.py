import json
import logging
import requests
from requests import HTTPError

logger_api = logging.getLogger("metax_api_requests")
logger_api.setLevel(logging.INFO)
file_handler_api = logging.FileHandler("metax_api_requests.log")
file_handler_api.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger_api.addHandler(file_handler_api)


METAX_BASE_URL = "https://metax-service.fd-staging.csc.fi/v3"
HEADERS = {"Content-Type": "application/json"}
KIELIPANKKI_CATALOG_ID = "urn:nbn:fi:att:data-catalog-kielipankki-v4"
TIMEOUT = 30

def check_if_dataset_record_in_datacatalog(dataset_pid):
    """
    Check if a dataset with given PID exists in given data catalog.
    :param dataset_pid: the persistent identifier of the resource
    :return: boolean
    """
    response = requests.get(
        f"{METAX_BASE_URL}/datasets?data_catalog__id={KIELIPANKKI_CATALOG_ID}&persistent_identifier={dataset_pid}",
        headers=HEADERS,
        timeout=TIMEOUT)
    return response.json()["count"] == 1 #Once Metax implements unique PIDs this check can be removed

def get_dataset_record_metax_id(dataset_pid):
    """
    Get the UUID of a dataset from Metax.
    :param dataset pid: the persistent identifier of the resource
    :return: the dataset identifier in Metax
    """
    response = requests.get(
        f"{METAX_BASE_URL}/datasets?data_catalog__id={KIELIPANKKI_CATALOG_ID}&persistent_identifier={dataset_pid}",
        headers=HEADERS,
        timeout=TIMEOUT)
    if response.json()["count"] == 1: #Once Metax implements unique PIDs this check can be removed
        return response.json()["results"][0]["id"]

def create_dataset(metadata_dict):
    """
    Create a dataset record to Metax.
    :param metadata_dict: dictionary of metadata mappings
    :return: the dataset identifier in Metax
    """
    response = requests.post(
        f"{METAX_BASE_URL}/datasets",
        json=metadata_dict,
        headers=HEADERS,
        timeout=TIMEOUT)
    try:
        response.raise_for_status()
    except HTTPError as error:
        logger_api.error("Error: %s. Failed to create dataset. Response text: %s ", error, response.text)
        raise
    logger_api.info("Created dataset. Response text: %s", response.text)
    return json.loads(response.text)["id"]

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
        timeout=TIMEOUT)
    try:
        response.raise_for_status()
    except HTTPError as error:
        logger_api.error("Error: %s. Failed to update catalog record %s", error, metax_dataset_id)
        raise
    logger_api.info("Updated dataset. Response text: %s", response.text)
    return json.loads(response.text)["id"]

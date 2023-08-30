import requests
from requests import HTTPError
import json
import logging

logging.basicConfig(filename='metax_api_requests.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


metax_base_url = "https://metax-service.fd-staging.csc.fi/v3"
headers = {'Content-Type': 'application/json'}
kielipankki_catalog_id = "urn:nbn:fi:att:data-catalog-kielipankki-v3"

def check_if_dataset_record_in_datacatalog(dataset_pid):
    """
    Check if a dataset with given PID exists in given data catalog.
    """
    r = requests.get(f"{metax_base_url}/datasets?data_catalog_id={kielipankki_catalog_id}&persistent_identifier={dataset_pid}", headers=headers)
    if r.json()["count"] == 1: #Once Metax implements unique PIDs this check can be removed
        return True

def get_dataset_record_metax_id(dataset_pid):
    """
    Get the UUID of a dataset from Metax.
    """
    r = requests.get(f"{metax_base_url}/datasets?data_catalog_id={kielipankki_catalog_id}&persistent_identifier={dataset_pid}", headers=headers)
    if r.json()["count"] == 1: #Once Metax implements unique PIDs this check can be removed
        return r.json()["results"][0]["id"]

def create_dataset(metadata_dict):
    """
    Create a dataset record to Metax.
    """
    r = requests.post(f"{metax_base_url}/datasets", json=metadata_dict, headers=headers)
    try:
        r.raise_for_status()
    except HTTPError as e:
        logging.error(f"Failed to create dataset. Response text: {r.text}")
        raise
    logging.info(f"Created dataset. Response text: {r.text}")
    return json.loads(r.text)['id']

def update_dataset(metax_dataset_id, metadata_dict):
    """
    Update existing dataset record in Metax.
    """
    r = requests.put(f"{metax_base_url}/datasets/{metax_dataset_id}", json=metadata_dict, headers=headers)
    try:
        r.raise_for_status()
    except HTTPError as e:
        logging.error(f'Failed to update catalog record {metax_dataset_id}')
        raise
    logging.info(f"Updated dataset. Response text: {r.text}")
    return json.loads(r.text)['id']
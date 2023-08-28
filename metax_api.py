import requests
from requests import HTTPError
import json


metax_base_url = "https://metax-service.fd-staging.csc.fi/v3"
headers = {'Content-Type': 'application/json'}
kielipankki_catalog_id = "urn:nbn:fi:att:data-catalog-kielipankki"

def check_if_dataset_record_in_datacatalog(dataset_pid):
    r = requests.get(f"{metax_base_url}/datasets?data_catalog{kielipankki_catalog_id}&persistent_identifier={dataset_pid}", headers=headers)
    if r.json()["count"] == 1:
        return True

def get_dataset_record_metax_id(dataset_pid):
    r = requests.get(f"{metax_base_url}/datasets?data_catalog{kielipankki_catalog_id}&persistent_identifier={dataset_pid}", headers=headers)
    if r.json()["count"] == 1:
        return r.json()["results"][0]["id"]

def create_dataset(metadata_json):
    r = requests.post(f"{metax_base_url}/datasets", json=metadata_json, headers=headers)
    try:
        r.raise_for_status()
    except HTTPError as e:
        raise
    return json.loads(r.text)['id']

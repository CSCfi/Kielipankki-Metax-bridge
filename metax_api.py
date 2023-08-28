import requests

metax_base_url = "https://metax-service.fd-staging.csc.fi/v3"
headers = {'Content-Type': 'application/json'}
kielipankki_catalog_id = "urn:nbn:fi:att:data-catalog-kielipankki"

def check_if_dataset_record_in_datacatalog(dataset_pid):
    r = requests.get(f"{metax_base_url}/datasets?data_catalog{kielipankki_catalog_id}&persistent_identifier={dataset_pid}", headers=headers)
    if r.json()["count"] == 1:
        return True

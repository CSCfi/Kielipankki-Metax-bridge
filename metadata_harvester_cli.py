"""
Main script for running metadata harvesting and sending it to Metax.
"""

from harvester.pmh_interface import PMH_API
from harvester.metadata_parser import MSRecordParser
import metax_api
from lxml import etree
from datetime import datetime, timedelta
import logging

logger_harvester = logging.getLogger("harvester")
logger_harvester.setLevel(logging.DEBUG)
file_handler_harvester = logging.FileHandler("harvester.log")
file_handler_harvester.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger_harvester.addHandler(file_handler_harvester)

def get_last_harvest_date():
    """Only successful harvests are logged. This function gets the last harvesting date from the log.
    """
    log_file_path = "harvester.log"
    with open(log_file_path, "r") as file:
        lines = file.readlines()
        if lines:
            log_date = lines[-1].split()[0]
            return log_date
        else:
            today = datetime.now().date()
            week_from_today = today - timedelta(days=7)
            return week_from_today.strftime("%Y-%m-%d")

def retrieve_metadata_content(url="https://kielipankki.fi/md_api/que"):
    """
    Fetch metadata records and convert and map them to Metax compliant dictionary.
    """
    api = PMH_API(url)
    all_mapped_data_dict = {}
    metadata_contents = api.get_all_metadata_records()
    for metadata_content in metadata_contents:
        lxml_record = etree.fromstring(etree.tostring(metadata_content.xml))
        metadata_record = MSRecordParser(lxml_record)
        if metadata_record.check_pid_exists():
            if metadata_record.check_resourcetype_corpus():
                pid = metadata_record.get_identifier("//info:identificationInfo/info:identifier/text()")
                all_mapped_data_dict[pid] = metadata_record.data_converter()
    return all_mapped_data_dict

def check_changes_from_last_week(kielipankki_record):
    """
    Check if harvested data has changes since last week.
    """
    modified_date = datetime.strptime(kielipankki_record["modified"], "%Y-%m-%dT%H:%M:%S.%fZ").date()
    today = datetime.now().date()
    week_from_today = today - timedelta(days=7)
    return week_from_today <= modified_date <= today

def send_data_to_metax(all_mapped_data_dict):
    """
    Make PUT and POST requests based on changes and existance of PIDs in Metax.
    """
    for pid in all_mapped_data_dict.keys():
        dataset_dict = all_mapped_data_dict[pid]
        if metax_api.check_if_dataset_record_in_datacatalog(pid):
            metax_dataset_id = metax_api.get_dataset_record_metax_id(pid)
            metax_api.update_dataset(metax_dataset_id, dataset_dict)
        else:
            metax_api.create_dataset(dataset_dict)



if __name__ == "__main__":
    send_data_to_metax(retrieve_metadata_content())
        
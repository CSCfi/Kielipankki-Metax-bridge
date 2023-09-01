"""
Main script for running metadata harvesting and sending it to Metax.
"""

from harvester.pmh_interface import PMH_API
from harvester.metadata_parser import MSRecordParser
import metax_api
from lxml import etree
import logging

logger_harvester = logging.getLogger("harvester")
logger_harvester.setLevel(logging.DEBUG)
file_handler_harvester = logging.FileHandler("harvester.log")
file_handler_harvester.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger_harvester.addHandler(file_handler_harvester)

def get_last_harvest_date():
    """Only successful harvests are logged. This function gets the last harvesting date from the log.
    :param filename: string value of a file name
    :return: date in last line
    """
    log_file_path = "harvester.log"
    with open(log_file_path, "r") as file:
        lines = file.readlines()
        if lines:
            log_date = lines[-1].split()[0]
            return log_date

def retrieve_metadata_content(url="https://kielipankki.fi/md_api/que"):
    """
    Fetches metadata records since the last logged harvest. If date is missing, all records are fetched.
    :param url: string value of a url
    :return: dictionary of mapped data
    """
    try:
        api = PMH_API(url)
        all_mapped_data_dict = {}
        if get_last_harvest_date("harvester.log"):
            metadata_contents = api.get_changed_records_from_last_harvest(get_last_harvest_date("harvester.log"))
        else:
            metadata_contents = api.get_all_metadata_records()

        for metadata_content in metadata_contents:
            lxml_record = etree.fromstring(etree.tostring(metadata_content.xml))
            metadata_record = MSRecordParser(lxml_record)
            if metadata_record.check_pid_exists():
                if metadata_record.check_resourcetype_corpus():
                    pid = metadata_record.get_identifier("//info:identificationInfo/info:identifier/text()")
                    all_mapped_data_dict[pid] = metadata_record.data_converter()
        return all_mapped_data_dict
    except:
        raise

def send_data_to_metax(all_mapped_data_dict):
    """
    Make PUT and POST requests based on changes and existance of PIDs in Metax.
    """
    if all_mapped_data_dict:
        try:
            for pid in all_mapped_data_dict.keys():
                dataset_dict = all_mapped_data_dict[pid]
                if metax_api.check_if_dataset_record_in_datacatalog(pid):
                    metax_dataset_id = metax_api.get_dataset_record_metax_id(pid)
                    metax_api.update_dataset(metax_dataset_id, dataset_dict)
                else:
                    metax_api.create_dataset(dataset_dict)
        except:
            raise
    else:
        pass


if __name__ == "__main__":
    try:
        send_data_to_metax(retrieve_metadata_content())
        logger_harvester.info("Success")
    except:
        raise
        
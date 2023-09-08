"""
Main script for running metadata harvesting and sending it to Metax.
"""

from harvester.pmh_interface import PMH_API
from harvester.metadata_parser import MSRecordParser
import metax_api
from lxml import etree
import logging
from datetime import datetime

logger_harvester = logging.getLogger("harvester")
logger_harvester.setLevel(logging.DEBUG)
file_handler_harvester = logging.FileHandler("harvester.log")
file_handler_harvester.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger_harvester.addHandler(file_handler_harvester)

def get_last_harvest_date():
    """Only successful harvests are logged. This function gets the last successful harvesting date from the log.
    :param filename: string value of a file name
    :return: date in last line
    """
    log_file_path = "harvester.log"
    try:
        with open(log_file_path, "r") as file:
            lines = file.readlines()

            for i in range(len(lines) - 1, -1, -1):
                if "success" in lines[i].lower():
                    if i > 0 and "started" in lines[i - 1].lower():
                        log_datetime_str = lines[i - 1].split(" - ")[0]
                        log_datetime = datetime.strptime(log_datetime_str, "%Y-%m-%d %H:%M:%S,%f")
                        return log_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
                    else:
                        continue # Continue until a successful harvest has been logged
        return None
    except FileNotFoundError:
        return None

def retrieve_metadata_content(url="https://kielipankki.fi/md_api/que"):
    """
    Fetches metadata records since the last logged harvest. If date is missing, all records are fetched.
    :param url: string value of a url
    :return: dictionary of mapped data
    """
    try:
        api = PMH_API(url)
        all_mapped_data_dict = {}
        if get_last_harvest_date():
            metadata_contents = api.get_changed_records_from_last_harvest(get_last_harvest_date())

        else:
            metadata_contents = api.get_all_metadata_records()

        if metadata_contents:
            for metadata_content in metadata_contents:
                lxml_record = etree.fromstring(etree.tostring(metadata_content.xml))
                metadata_record = MSRecordParser(lxml_record)
                if metadata_record.check_pid_exists():
                    if metadata_record.check_resourcetype_corpus():
                        pid = metadata_record.get_identifier("//info:identificationInfo/info:identifier/text()")
                        all_mapped_data_dict[pid] = metadata_record.to_dict()
            return all_mapped_data_dict
    except:
        raise

def send_data_to_metax(all_mapped_data_dict):
    """
    Make PUT and POST requests based on changes and existance of PIDs in Metax.
    :param all_mapped_data_dict: a dictionary of mapped data
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
    last_harvest_date = get_last_harvest_date()
    logger_harvester.info("Started")
    try:
        send_data_to_metax(retrieve_metadata_content())
        if last_harvest_date:
            logger_harvester.info(f"Success, records harvested since {last_harvest_date}")
        else:
            logger_harvester.info("Success, all records harvested")
    except:
        raise
        
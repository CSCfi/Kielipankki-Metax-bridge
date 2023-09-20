"""
Main script for running metadata harvesting and sending it to Metax.
"""

import logging
from datetime import datetime
from lxml import etree
from harvester.pmh_interface import PMH_API
from harvester.metadata_parser import MSRecordParser
import metax_api

logger_harvester = logging.getLogger("harvester")
logger_harvester.setLevel(logging.DEBUG)
file_handler_harvester = logging.FileHandler("harvester.log")
file_handler_harvester.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)
logger_harvester.addHandler(file_handler_harvester)


def last_harvest_date(filename):
    """This function gets the start time of last successful harvesting date and time from the log
    if found.
    :param filename: string value of a file name
    :return: date and time
    """
    try:
        with open(filename, "r") as file:
            lines = file.readlines()

            for i in range(len(lines) - 1, -1, -1):
                if "success" in lines[i].lower():
                    if i > 0 and "started" in lines[i - 1].lower():
                        log_datetime_str = lines[i - 1].split(" - ")[0]
                        log_datetime = datetime.strptime(
                            log_datetime_str, "%Y-%m-%d %H:%M:%S,%f"
                        )
                        return log_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
        return None
    except FileNotFoundError:
        return None


def records_to_dict(date_time, url="https://kielipankki.fi/md_api/que"):
    """
    Fetches metadata records since the last logged harvest. If date is missing, all records are
    fetched.
    :param url: string value of a url
    :return: dictionary of mapped data
    """
    api = PMH_API(url)
    all_mapped_data_dict = {}
    if date_time:
        metadata_contents = api.fetch_changed_records(date_time)
    else:
        metadata_contents = api.fetch_records()

    if metadata_contents:
        for metadata_content in metadata_contents:
            lxml_record = etree.fromstring(
                etree.tostring(metadata_content.xml))
            metadata_record = MSRecordParser(lxml_record)
            if metadata_record.check_pid_exists():
                if metadata_record.check_resourcetype_corpus():
                    pid = metadata_record.get_identifier(
                        "//info:identificationInfo/info:identifier/text()"
                    )
                    all_mapped_data_dict[pid] = metadata_record.to_dict()
    return all_mapped_data_dict


def send_data_to_metax(all_mapped_data_dict):
    """
    Make PUT and POST requests based on changes and existance of PIDs in Metax.
    :param all_mapped_data_dict: a dictionary of mapped data
    """
    if all_mapped_data_dict:
        for pid in all_mapped_data_dict.keys():
            dataset_dict = all_mapped_data_dict[pid]
            if metax_api.check_if_dataset_record_in_datacatalog(pid):
                metax_dataset_id = metax_api.get_dataset_record_metax_id(pid)
                metax_api.update_dataset(metax_dataset_id, dataset_dict)
            else:
                metax_api.create_dataset(dataset_dict)
    else:
        pass


def sync_deleted_records(url="https://kielipankki.fi/md_api/que"):
    """
    Compares record PIDs fetched from Kielipankki and Metax. Any records not existing in Kielipankki (anymore) are deleted from Metax as well.
    """
    api = PMH_API(url)
    kielipankki_pids = []
    metadata_contents = api.fetch_records()
    for metadata_content in metadata_contents:
        lxml_record = etree.fromstring(
            etree.tostring(metadata_content.xml))
        metadata_record = MSRecordParser(lxml_record)
        if metadata_record.check_pid_exists():
            if metadata_record.check_resourcetype_corpus():
                pid = metadata_record.get_identifier(
                    "//info:identificationInfo/info:identifier/text()")
                kielipankki_pids.append(pid)

    metax_pids = set(metax_api.datacatalog_dataset_record_pids())
    pids_not_in_kielipankki = metax_pids.difference(set(kielipankki_pids))
    for pid in pids_not_in_kielipankki:
        metax_api.delete_dataset(metax_api.get_dataset_record_metax_id(pid))


def main(log_file):
    """
    Runs the whole pipeline of fetching data since last harvest and sending it to Metax.
    :param log_file: log file where harves dates are logged
    """
    harvested_date = last_harvest_date(log_file)
    logger_harvester.info("Started")
    send_data_to_metax(records_to_dict(harvested_date))
    if harvested_date:
        logger_harvester.info(
            "Success, records harvested since %s", harvested_date)
    else:
        logger_harvester.info("Success, all records harvested")

    sync_deleted_records()


if __name__ == "__main__":
    main("harvester.log")

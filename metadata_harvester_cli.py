"""
Main script for running metadata harvesting and sending it to Metax.
"""

import logging
from datetime import datetime
from lxml import etree
from harvester.pmh_interface import PMH_API
from harvester.metadata_parser import MSRecordParser
from metax_api import MetaxAPI

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


def records_to_dict(date_time=None, url="https://kielipankki.fi/md_api/que"):
    """
    Fetches metadata records since the last logged harvest. If date is missing, all records are
    fetched.
    :param: date_time: date and time value in format that OAI PMH reads
    :param url: string value of a url
    :return: list of mapped Metashare records (dictionaries)
    """
    api = PMH_API(url)
    if not date_time:
        metashare_records = api.fetch_records()
    metashare_records = api.fetch_changed_records(date_time)

    mapped_records_list = []
    if metashare_records:
        for metashare_record in metashare_records:
            lxml_record = etree.fromstring(
                etree.tostring(metashare_record.xml))
            mapped_record = MSRecordParser(lxml_record)
            if (
                mapped_record.check_pid_exists()
                and mapped_record.check_resourcetype_corpus()
            ):
                mapped_records_list.append(mapped_record.to_dict())
    return mapped_records_list


def send_data_to_metax(mapped_records):
    """
    Make PUT and POST requests based on changes and existance of PIDs in Metax.
    :param mapped_records: a list of mapped Metashare records (dictionaries)
    """
    if mapped_records:
        metax_api = MetaxAPI()
        for mapped_record in mapped_records:
            pid = mapped_record["persistent_identifier"]
            if metax_api.record_id(pid):
                metax_api.update_record(
                    metax_api.record_id(pid), mapped_record)
            else:
                metax_api.create_record(mapped_record)
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

    metax_api = MetaxAPI()
    metax_pids = set(metax_api.datacatalog_record_pids())
    pids_not_in_kielipankki = metax_pids.difference(set(kielipankki_pids))
    for pid in pids_not_in_kielipankki:
        metax_api.delete_record(metax_api.record_id(pid))


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

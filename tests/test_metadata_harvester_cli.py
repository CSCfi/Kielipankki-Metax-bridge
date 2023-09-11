import pytest
import requests
import requests_mock
from lxml import etree
import metadata_harvester_cli
import os

@pytest.fixture
def kielipankki_api_url():
    """
    The URL of the OAI-PMH API used in tests.
    """
    return "https://kielipankki.fi/md_api/que?metadataPrefix=info&verb=ListRecords"

def _get_file_as_string(filename):
    with open(filename) as infile:
        return infile.read()

@pytest.fixture
def single_record_xml():
    return _get_file_as_string("tests/test_data/kielipankki_record_sample.xml")

@pytest.fixture
def single_record_response(kielipankki_api_url, single_record_xml):
    """
    A GET request that returns the XML data as a dictionary
    """
    
    with requests_mock.Mocker() as mocker:
        mocker.get(kielipankki_api_url, text=single_record_xml)
        yield {"urn.fi/urn:nbn:fi:lb-2017021609": {"data_catalog": "urn:nbn:fi:att:data-catalog-kielipankki-v3", "language": [{"url": "http://lexvo.org/id/iso639-3/fin"}], "field_of_science": [{"url": "http://www.yso.fi/onto/okm-tieteenala/ta112"}], "persistent_identifier": "urn.fi/urn:nbn:fi:lb-2017021609", "title": {"en": "Silva Kiuru's Time Expressions Corpus", "fi": "Silva Kiurun ajanilmausaineisto"}, "description": {"en": "This corpus of time expressions has been compiled from literary works, translations, dialect texts as well as other texts. Format: word documents.", "fi": "Tämä suomen kielen ajanilmauksia käsittävä aineisto on koottu kaunokirjallisten alkuperäisteosten, käännösten, murreaineistojen ja muiden tekstien pohjalta."},"modified": "2017-02-15T00:00:00.000000Z", "issued": "2017-02-15T00:00:00.000000Z", "access_rights": {"license": [{"url": "http://uri.suomi.fi/codelist/fairdata/license/code/undernegotiation"}], "access_type": {"url": "http://uri.suomi.fi/codelist/fairdata/access_type/code/open"}}}}

def test_retrieve_metadata_content_with_last_harvest_date(single_record_response, create_test_log_file):
    """
    Test that fetching records based on a date in log file succeeds (only updated records are fetched).
    """
    result = metadata_harvester_cli.retrieve_metadata_content("harvester_test.log")    
    assert single_record_response == result

def test_retrieve_metadata_content_without_last_harvest_date(single_record_response):
    """
    Test that fetching records without a log file succeeds (all records are fetched).
    """
    result = metadata_harvester_cli.retrieve_metadata_content("harvester_test.log")
    assert single_record_response == result

@pytest.fixture
def create_test_log_file():
    """Create a temporary log file for testing and clean up afterwards."""
    log_file = "harvester_test.log"
    log_file_data = [
        "2023-09-08 14:34:16,887 - INFO - Started\n"
        "2023-09-08 14:42:58,652 - INFO - Success, all records harvested\n"
        "2023-09-08 14:44:58,690 - INFO - Started\n"
        "2023-09-08 14:45:58,690 - INFO - Started\n"
        "2023-09-08 14:45:58,956 - INFO - Success, records harvested since 2023-09-08T14:34:16Z\n"
    ]
    with open(log_file, "w") as file:
        file.writelines(log_file_data)
    yield log_file
    os.remove(log_file)

def test_get_last_harvest_date(create_test_log_file):
    """Test getting the last start time of successful harvest date in the log file"""
    last_harvest_date =metadata_harvester_cli.get_last_harvest_date(create_test_log_file)
    assert last_harvest_date == "2023-09-08T14:45:58Z"

def test_get_last_harvest_no_file():
    """Test handling non-existing file."""
    last_harvest_date =metadata_harvester_cli.get_last_harvest_date("harvester_test.log")
    assert last_harvest_date == None

@pytest.fixture
def create_test_log_file_with_unsuccessful_harvests():
    """Create a temporary log file for testing and clean up afterwards."""
    log_file = "harvester_test.log"
    log_file_data = [
        "2023-09-08 14:34:16,887 - INFO - Started\n"
        "2023-09-08 14:42:58,652 - INFO - Success, all records harvested\n"
        "2023-09-08 14:44:58,690 - INFO - Started\n"
        "2023-09-08 14:45:58,690 - INFO - Started\n"
        "2023-09-08 14:45:58,956 - INFO - Started\n"
    ]
    with open(log_file, "w") as file:
        file.writelines(log_file_data)
    yield log_file
    os.remove(log_file)

def test_get_last_harvest_with_unsuccessful_harvests(create_test_log_file_with_unsuccessful_harvests):
    """Test getting the last start time of successful harvest date in the log file"""
    last_harvest_date =metadata_harvester_cli.get_last_harvest_date(create_test_log_file_with_unsuccessful_harvests)
    assert last_harvest_date == "2023-09-08T14:34:16Z"

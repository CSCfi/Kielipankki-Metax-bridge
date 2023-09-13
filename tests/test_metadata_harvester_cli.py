import os
import pytest
import requests_mock
import metadata_harvester_cli


@pytest.fixture
def kielipankki_api_url():
    """
    The URL of the OAI-PMH API used in tests.
    """
    return "https://kielipankki.fi/md_api/que?metadataPrefix=info&verb=ListRecords"


def _get_file_as_string(filename):
    """Return given file as string."""
    with open(filename) as infile:
        return infile.read()


@pytest.fixture
def single_record_xml():
    """Well-formed sample xml"""
    return _get_file_as_string("tests/test_data/kielipankki_record_sample.xml")


@pytest.fixture
def single_record_to_dict(kielipankki_api_url, single_record_xml):
    """
    A GET request that returns the XML data as a dictionary
    """
    with requests_mock.Mocker() as mocker:
        mocker.get(kielipankki_api_url, text=single_record_xml)
        yield {"urn.fi/urn:nbn:fi:lb-2017021609": {"data_catalog": "urn:nbn:fi:att:data-catalog-kielipankki-v4", "language": [{"url": "http://lexvo.org/id/iso639-3/fin"}], "field_of_science": [{"url": "http://www.yso.fi/onto/okm-tieteenala/ta112"}], "persistent_identifier": "urn.fi/urn:nbn:fi:lb-2017021609", "title": {"en": "Silva Kiuru's Time Expressions Corpus", "fi": "Silva Kiurun ajanilmausaineisto"}, "description": {"en": "This corpus of time expressions has been compiled from literary works, translations, dialect texts as well as other texts. Format: word documents.", "fi": "Tämä suomen kielen ajanilmauksia käsittävä aineisto on koottu kaunokirjallisten alkuperäisteosten, käännösten, murreaineistojen ja muiden tekstien pohjalta."}, "modified": "2017-02-15T00:00:00.000000Z", "issued": "2017-02-15T00:00:00.000000Z", "access_rights": {"license": [{"url": "http://uri.suomi.fi/codelist/fairdata/license/code/undernegotiation"}], "access_type": {"url": "http://uri.suomi.fi/codelist/fairdata/access_type/code/open"}}}}


def test_records_to_dict_with_last_harvest_date(single_record_to_dict, create_test_log_file):
    """
    Test that fetching records based on a date in log file succeeds (only updated records are
    fetched).
    """
    result = metadata_harvester_cli.records_to_dict(create_test_log_file)
    assert single_record_to_dict == result


def test_records_to_dict_without_last_harvest_date(single_record_to_dict):
    """
    Test that fetching records without a log file succeeds (all records are fetched).
    """
    result = metadata_harvester_cli.records_to_dict("harvester_test.log")
    assert single_record_to_dict == result


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


def test_last_harvest_date(create_test_log_file):
    """Test getting the last start time of successful harvest date in the log file"""
    harvested_date = metadata_harvester_cli.last_harvest_date(
        create_test_log_file)
    assert harvested_date == "2023-09-08T14:45:58Z"


def test_get_last_harvest_no_file():
    """Test handling non-existing file."""
    harvested_date = metadata_harvester_cli.last_harvest_date(
        "harvester_test.log")
    assert harvested_date is None


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
    harvested_date = metadata_harvester_cli.last_harvest_date(
        create_test_log_file_with_unsuccessful_harvests)
    assert harvested_date == "2023-09-08T14:34:16Z"


def test_send_data_to_metax(mock_requests_get_no_pid, single_record_to_dict, mock_requests_post):
    """Test creating a record in Metax from a dictionary."""
    metadata_harvester_cli.send_data_to_metax(single_record_to_dict)
    assert len(mock_requests_get_no_pid.request_history) == 1
    assert mock_requests_get_no_pid.request_history[0].method == "GET"
    assert mock_requests_get_no_pid.request_history[0].url == "https://" \
        "metax-service.fd-staging.csc.fi/v3/datasets?" \
        "data_catalog_id=urn:nbn:fi:att:data-catalog-kielipankki-v4" \
        "&persistent_identifier=urn.fi/urn:nbn:fi:lb-2017021609"
    assert len(mock_requests_post.request_history) == 1
    assert mock_requests_post.request_history[0].method == "POST"
    assert mock_requests_post.request_history[0].url == "https://" \
        "metax-service.fd-staging.csc.fi/v3/datasets"

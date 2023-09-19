import os
import pytest
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
def single_record_to_dict(shared_request_mocker, kielipankki_api_url, single_record_xml):
    """
    A GET request that returns the XML data as a dictionary
    """
    shared_request_mocker.get(kielipankki_api_url, text=single_record_xml)
    yield {"urn.fi/urn:nbn:fi:lb-2017021609": {"data_catalog": "urn:nbn:fi:att:data-catalog-kielipankki-v4", "language": [{"url": "http://lexvo.org/id/iso639-3/fin"}], "field_of_science": [{"url": "http://www.yso.fi/onto/okm-tieteenala/ta112"}], "persistent_identifier": "urn.fi/urn:nbn:fi:lb-2017021609", "title": {"en": "Silva Kiuru's Time Expressions Corpus", "fi": "Silva Kiurun ajanilmausaineisto"}, "description": {"en": "This corpus of time expressions has been compiled from literary works, translations, dialect texts as well as other texts. Format: word documents.", "fi": "Tämä suomen kielen ajanilmauksia käsittävä aineisto on koottu kaunokirjallisten alkuperäisteosten, käännösten, murreaineistojen ja muiden tekstien pohjalta."}, "modified": "2017-02-15T00:00:00.000000Z", "issued": "2017-02-15T00:00:00.000000Z", "access_rights": {"license": [{"url": "http://uri.suomi.fi/codelist/fairdata/license/code/undernegotiation"}], "access_type": {"url": "http://uri.suomi.fi/codelist/fairdata/access_type/code/open"}}}}


def test_records_to_dict_with_last_harvest_date(single_record_to_dict, create_test_log_file):
    """
    Test that fetching records based on a date in log file succeeds (only updated records are
    fetched).
    """
    date = metadata_harvester_cli.last_harvest_date(create_test_log_file)
    result = metadata_harvester_cli.records_to_dict(date)
    assert date == "2023-09-08T14:45:58Z"
    assert single_record_to_dict == result


def test_records_to_dict_without_last_harvest_date(single_record_to_dict, create_test_log_file_with_unsuccessful_harvest):
    """
    Test that fetching records without a log file succeeds (all records are fetched).
    """
    date = metadata_harvester_cli.last_harvest_date(create_test_log_file_with_unsuccessful_harvest)
    result = metadata_harvester_cli.records_to_dict(date)
    assert date is None
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


@pytest.fixture
def create_test_log_file_with_unsuccessful_harvest():
    """Create a temporary log file with no successfull harvests and clean up afterwards."""
    log_file = "harvester_test.log"
    log_file_data = [
        "2023-09-08 14:34:16,887 - INFO - Started\n"
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
def create_test_log_file_with_one_successful_harvest():
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


def test_get_last_harvest_with_unsuccessful_harvest(
    create_test_log_file_with_one_successful_harvest):
    """Test getting the last start time of successful harvest date in the log file"""
    harvested_date = metadata_harvester_cli.last_harvest_date(
        create_test_log_file_with_one_successful_harvest)
    assert harvested_date == "2023-09-08T14:34:16Z"


def test_send_data_to_metax_single_new_record(
    single_record_to_dict,
    mock_metashare_record_not_found_in_datacatalog,
    mock_requests_post,
):
    """
    Check that creating one new metadata record works

    This means that Metax is queried for existence of the PID (not found) and then a new
    record is POSTed. We also check that the posted data corresponds to the record dict
    passed to the function.
    """
    metadata_harvester_cli.send_data_to_metax(single_record_to_dict)

    assert mock_requests_post.call_count == 2

    expected_post_request = mock_requests_post.request_history[1]

    assert expected_post_request.method == "POST"
    assert expected_post_request.json() == list(single_record_to_dict.values())[0]


def test_send_data_to_metax_single_pre_existing_record(
    single_record_to_dict,
    mock_metashare_record_found_in_datacatalog,
    mock_requests_put,
):
    """
    Check that creating one new metadata record works

    This means that Metax is queried for existence of the PID (found), fetches the Metax
    ID for the record, and then a new record is PUT. We also check that the posted
    data corresponds to the record dict passed to the function.
    """
    metadata_harvester_cli.send_data_to_metax(single_record_to_dict)

    assert mock_requests_put.call_count == 3

    expected_put_request = mock_requests_put.request_history[2]

    assert expected_put_request.method == "PUT"
    assert expected_put_request.json() == list(single_record_to_dict.values())[0]


def test_send_data_to_metax_multiple_records(
    mock_metashare_record_not_found_in_datacatalog,
    mock_requests_post,
):
    """
    Check that send_data_to_metax can handle more than one record at once.

    This test only covers totally new records, as other tests are assumed to ensure that
    updating old records works. Thus we expect to see one GET (check whether the record
    already exists in Metax) and one POST (add the new record) for each given item in
    the metadata records. This test also checks that teach POST has unique data.
    """
    record_dict = {
        "pid1": {"meta": "data", "id": "pid1"},
        "pid2": {"infor": "mation", "id": "pid2"},
    }
    metadata_harvester_cli.send_data_to_metax(record_dict)

    assert mock_requests_post.call_count == 4

    post_requests = mock_requests_post.request_history[1::2]
    for post_request, record in zip(post_requests, record_dict.values()):
        assert post_request.method == "POST"
        assert post_request.json()["id"] == record["id"]


def test_send_data_to_metax_no_records_post(shared_request_mocker, metax_base_url):
    """
    Check that send_data_to_metax does not POST if en empty dictinary is passed to it.
    """
    record_dict = {}
    metadata_harvester_cli.send_data_to_metax(record_dict)
    shared_request_mocker.post(metax_base_url)

    assert shared_request_mocker.call_count == 0


def test_send_data_to_metax_no_records_put(shared_request_mocker, metax_base_url):
    """
    Check that send_data_to_metax does not PUT if an empty dictionary is passed to it.
    """
    record_dict = {}
    metadata_harvester_cli.send_data_to_metax(record_dict)
    shared_request_mocker.put(metax_base_url)

    assert shared_request_mocker.call_count == 0




def test_main_all_data_harvested(
    mock_requests_post,
    mock_metashare_record_not_found_in_datacatalog,
    single_record_to_dict):
    """
    Check that when no successful harvest date is available, all data is fetched from Kielipankki
    and then sent to Metax.
    """
    metadata_harvester_cli.main()

    assert mock_requests_post.call_count == 3
    assert mock_requests_post.request_history[2].method == "POST"
    assert mock_requests_post.request_history[2].json()["persistent_identifier"] == list(single_record_to_dict.values())[0]["persistent_identifier"]


def test_main_new_records_harvested_since_date(
    mock_requests_post,
    mock_metashare_record_not_found_in_datacatalog,
    single_record_to_dict,
    create_test_log_file):
    """
    Check that, when there is a successful harvest logged, new and updated records since that 
    date are fetched from Kielipankki and then sent to Metax. 
    
    This test covers POSTing new records.
    """
    metadata_harvester_cli.main()

    assert mock_requests_post.call_count == 3
    assert mock_requests_post.request_history[2].method == "POST"
    assert mock_requests_post.request_history[2].json()["persistent_identifier"] == list(single_record_to_dict.values())[0]["persistent_identifier"]


def test_main_changed_records_harvested_since_date(
    mock_requests_put,
    mock_metashare_record_found_in_datacatalog,
    single_record_to_dict,
    create_test_log_file):
    """
    Check that, when there is a successful harvest logged previously, new and updated records
    are fetched from Kielipankki and then sent to Metax.

    This test covers only changed records that are PUT to Metax.
    """
    metadata_harvester_cli.main()

    assert mock_requests_put.call_count == 4
    assert mock_requests_put.request_history[3].method == "PUT"
    assert mock_requests_put.request_history[3].json()["persistent_identifier"] == list(single_record_to_dict.values())[0]["persistent_identifier"]

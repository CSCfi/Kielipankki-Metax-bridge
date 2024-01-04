import logging
import pytest
import metadata_harvester_cli


@pytest.fixture
def single_record_to_dict(
    shared_request_mocker, kielipankki_api_url, metashare_single_record_xml
):
    """
    A GET request that returns the XML data as a dictionary
    """
    shared_request_mocker.get(kielipankki_api_url, text=metashare_single_record_xml)
    yield [
        {
            "data_catalog": "urn:nbn:fi:att:data-catalog-kielipankki",
            "language": [{"url": "http://lexvo.org/id/iso639-3/fin"}],
            "field_of_science": [
                {"url": "http://www.yso.fi/onto/okm-tieteenala/ta112"}
            ],
            "persistent_identifier": "urn.fi/urn:nbn:fi:lb-2016101210",
            "title": {
                "en": "Silva Kiuru's Time Expressions Corpus",
                "fi": "Silva Kiurun ajanilmausaineisto",
            },
            "description": {
                "en": "This corpus of time expressions has been compiled from literary works, translations, dialect texts as well as other texts. Format: word documents.",
                "fi": "Tämä suomen kielen ajanilmauksia käsittävä aineisto on koottu kaunokirjallisten alkuperäisteosten, käännösten, murreaineistojen ja muiden tekstien pohjalta.",
            },
            "modified": "2017-02-15T00:00:00.000000Z",
            "issued": "2017-02-15T00:00:00.000000Z",
            "access_rights": {
                "license": [
                    {
                        "url": "http://uri.suomi.fi/codelist/fairdata/license/code/undernegotiation"
                    }
                ],
                "access_type": {
                    "url": "http://uri.suomi.fi/codelist/fairdata/access_type/code/open"
                },
            },
        }
    ]


def test_records_to_dict_with_last_harvest_date(
    single_record_to_dict, create_test_log_file
):
    """
    Test that fetching records based on a date in log file succeeds (only updated records are
    fetched).
    """
    date = metadata_harvester_cli.last_harvest_date(create_test_log_file)
    result = metadata_harvester_cli.records_to_dict(date)
    assert date == "2023-09-08T14:45:58Z"
    assert single_record_to_dict == result


def test_records_to_dict_without_last_harvest_date(
    single_record_to_dict, create_test_log_file_with_unsuccessful_harvest
):
    """
    Test that fetching records without a log file succeeds (all records are fetched).
    """
    date = metadata_harvester_cli.last_harvest_date(
        create_test_log_file_with_unsuccessful_harvest
    )
    result = metadata_harvester_cli.records_to_dict(date)
    assert date is None
    assert single_record_to_dict == result


@pytest.fixture
def create_test_log_file(tmp_path):
    """Create a temporary log file for testing and clean up afterwards."""
    # log_file = "harvester_test.log"
    log_file_data = [
        "2023-09-08 14:34:16,887 - INFO - Started\n"
        "2023-09-08 14:42:58,652 - INFO - Success, all records harvested\n"
        "2023-09-08 14:44:58,690 - INFO - Started\n"
        "2023-09-08 14:45:58,690 - INFO - Started\n"
        "2023-09-08 14:45:58,956 - INFO - Success, records harvested since 2023-09-08T14:34:16Z\n"
    ]
    log_file = tmp_path / "harvester_test.log"
    with open(log_file, "w") as file:
        file.writelines(log_file_data)
    return log_file


@pytest.fixture
def create_test_log_file_with_unsuccessful_harvest(tmp_path):
    """Create a temporary log file with no successfull harvests and clean up afterwards."""
    log_file_data = ["2023-09-08 14:34:16,887 - INFO - Started\n"]
    log_file = tmp_path / "harvester_test.log"
    with open(log_file, "w") as file:
        file.writelines(log_file_data)
    return log_file


def test_last_harvest_date(create_test_log_file):
    """Test getting the last start time of successful harvest date in the log file"""
    harvested_date = metadata_harvester_cli.last_harvest_date(create_test_log_file)
    assert harvested_date == "2023-09-08T14:45:58Z"


def test_get_last_harvest_no_file():
    """Test handling non-existing file."""
    harvested_date = metadata_harvester_cli.last_harvest_date("harvester_test.log")
    assert harvested_date is None


@pytest.fixture
def create_test_log_file_with_one_successful_harvest(tmp_path):
    """Create a temporary log file for testing and clean up afterwards."""
    # log_file = "harvester_test.log"
    log_file_data = [
        "2023-09-08 14:34:16,887 - INFO - Started\n"
        "2023-09-08 14:42:58,652 - INFO - Success, all records harvested\n"
        "2023-09-08 14:44:58,690 - INFO - Started\n"
        "2023-09-08 14:45:58,690 - INFO - Started\n"
        "2023-09-08 14:45:58,956 - INFO - Started\n"
    ]
    log_file = tmp_path / "harvester_test.log"
    with open(log_file, "w") as file:
        file.writelines(log_file_data)
    return log_file


def test_get_last_harvest_with_unsuccessful_harvest(
    create_test_log_file_with_one_successful_harvest,
):
    """Test getting the last start time of successful harvest date in the log file"""
    harvested_date = metadata_harvester_cli.last_harvest_date(
        create_test_log_file_with_one_successful_harvest
    )
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
    assert expected_post_request.json() == single_record_to_dict[0]


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
    assert expected_put_request.json() == single_record_to_dict[0]


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
    records = [
        {"meta": "data", "persistent_identifier": "pid1"},
        {"infor": "mation", "persistent_identifier": "pid2"},
    ]
    metadata_harvester_cli.send_data_to_metax(records)

    assert mock_requests_post.call_count == 4

    post_requests = mock_requests_post.request_history[1::2]
    for post_request, record in zip(post_requests, records):
        assert post_request.method == "POST"
        assert (
            post_request.json()["persistent_identifier"]
            == record["persistent_identifier"]
        )


def test_send_data_to_metax_no_records_post(shared_request_mocker, metax_base_url):
    """
    Check that send_data_to_metax does not POST if en empty dictinary is passed to it.
    """
    records = []
    metadata_harvester_cli.send_data_to_metax(records)
    shared_request_mocker.post(metax_base_url)

    assert shared_request_mocker.call_count == 0


def test_send_data_to_metax_no_records_put(shared_request_mocker, metax_base_url):
    """
    Check that send_data_to_metax does not PUT if an empty dictionary is passed to it.
    """
    records = []
    metadata_harvester_cli.send_data_to_metax(records)
    shared_request_mocker.put(metax_base_url)

    assert shared_request_mocker.call_count == 0


def test_collect_metax_pids(mock_pids_list_in_datacatalog):
    """
    Test that a list of PIDs from Metax are returned.
    """
    pids = metadata_harvester_cli.collect_metax_pids()
    assert pids == mock_pids_list_in_datacatalog


def test_sync_deleted_records_with_diffs(
    mock_single_pid_list_from_metashare,
    mock_pids_list_in_datacatalog,
    mock_metashare_record_found_in_datacatalog,
    mock_delete_record,
):
    """
    Test that when PIDs collected from Metax do not exist in Metashare, those records are deleted from Metax.
    """
    metadata_harvester_cli.sync_deleted_records(
        mock_single_pid_list_from_metashare, mock_pids_list_in_datacatalog
    )
    assert mock_delete_record.call_count == 2


def test_sync_deleted_records_no_diffs(
    mock_single_pid_list_from_metashare,
    mock_pids_list_in_datacatalog_matching_metashare,
    mock_delete_record,
):
    """
    Test that when PIDs collected from Metax and Metashare match, no DELETE requests are made.
    """
    metadata_harvester_cli.sync_deleted_records(
        mock_single_pid_list_from_metashare,
        mock_pids_list_in_datacatalog_matching_metashare,
    )
    assert mock_delete_record.call_count == 0


def test_main_all_data_harvested_and_records_in_sync(
    mock_requests_post,
    mock_metashare_record_not_found_in_datacatalog,
    single_record_to_dict,
    create_test_log_file_with_unsuccessful_harvest,
    caplog,
    mock_single_pid_list_from_metashare,
    mock_pids_list_in_datacatalog_matching_metashare,
):
    """
    Check that when no successful harvest date is available (the log file does not have any successful harvests logged), all data is fetched from Kielipankki.

    The test also covers the situation where none of the record PID matches the ones in Metax so the data is POSTed to Metax.

    Finally, the records from both services are compared and no diffs are found so they are in sync.
    """
    with caplog.at_level(logging.INFO):
        metadata_harvester_cli.main(create_test_log_file_with_unsuccessful_harvest)

    assert mock_requests_post.call_count == 5
    assert mock_requests_post.request_history[2].method == "POST"
    assert (
        mock_requests_post.request_history[2].json()["persistent_identifier"]
        == single_record_to_dict[0]["persistent_identifier"]
    )

    assert "Success, all records harvested" in caplog.text


def test_main_all_data_harvested_and_records_not_in_sync(
    mock_requests_put,
    single_record_to_dict,
    create_test_log_file_with_unsuccessful_harvest,
    caplog,
    mock_single_pid_list_from_metashare,
    mock_pids_list_in_datacatalog,
    mock_metashare_record_found_in_datacatalog,
    mock_delete_record,
):
    """
    Check that when no successful harvest date is available (the log file does not have any successful harvests logged), all data is fetched from Kielipankki.

    The test also covers the situation where the record PID matches the ones in Metax so the data is PUT to Metax.

    Finally, the records from both services are compared resulting in one non-matching PID. This non-matching record is then DELETEd from Metax.
    """

    with caplog.at_level(logging.INFO):
        metadata_harvester_cli.main(create_test_log_file_with_unsuccessful_harvest)

    assert mock_requests_put.call_count == 6
    assert mock_requests_put.request_history[3].method == "PUT"
    assert (
        mock_requests_put.request_history[3].json()["persistent_identifier"]
        == single_record_to_dict[0]["persistent_identifier"]
    )

    assert "Success, all records harvested" in caplog.text


def test_main_new_records_harvested_since_date_and_records_in_sync(
    mock_requests_post,
    single_record_to_dict,
    create_test_log_file,
    caplog,
    mock_single_pid_list_from_metashare,
    mock_pids_list_in_datacatalog_matching_metashare,
    mock_delete_record,
    mock_metashare_record_not_found_in_datacatalog,
):
    """
    Check that, when there is a successful harvest logged, new and updated records since that
    date are fetched from Kielipankki and then sent to Metax.

    This test covers POSTing new records.

    Finally, the records from both services are compared and no diffs are found so they are in sync.
    """
    with caplog.at_level(logging.INFO):
        metadata_harvester_cli.main(create_test_log_file)

    assert mock_requests_post.call_count == 5
    assert mock_requests_post.request_history[2].method == "POST"
    assert (
        mock_requests_post.request_history[2].json()["persistent_identifier"]
        == single_record_to_dict[0]["persistent_identifier"]
    )

    assert "Success, records harvested since" in caplog.text


def test_main_changed_records_harvested_since_date_and_records_not_in_sync(
    mock_requests_put,
    mock_metashare_record_found_in_datacatalog,
    single_record_to_dict,
    create_test_log_file,
    caplog,
    mock_single_pid_list_from_metashare,
    mock_pids_list_in_datacatalog,
    mock_delete_record,
):
    """
    Check that, when there is a successful harvest logged previously, new and updated records
    are fetched from Kielipankki and then sent to Metax.

    This test covers only changed records that are PUT to Metax.

    Finally, the records from both services are compared resulting in one non-matching PID. This non-matching record is then DELETEd from Metax.

    """
    with caplog.at_level(logging.INFO):
        metadata_harvester_cli.main(create_test_log_file)

    assert mock_requests_put.call_count == 8
    assert mock_requests_put.request_history[3].method == "PUT"
    assert (
        mock_requests_put.request_history[3].json()["persistent_identifier"]
        == single_record_to_dict[0]["persistent_identifier"]
    )

    assert "Success, records harvested since" in caplog.text

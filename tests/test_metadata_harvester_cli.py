import logging
import os

import pytest

import metadata_harvester_cli


# Pylint does not understand fixture use
# pylint: disable=redefined-outer-name


@pytest.fixture
def create_test_log_file(latest_harvest_timestamp, default_test_log_file_path):
    """Create a temporary log file for testing and clean up afterwards."""
    # log_file = "harvester_test.log"
    log_file_data = [
        "2023-09-08 14:34:16,887 - INFO - Started\n"
        "2023-09-08 14:42:58,652 - INFO - Success, all records harvested\n"
        "2023-09-08 14:44:58,690 - INFO - Started\n"
        "2023-09-08 14:45:58,690 - INFO - Started\n"
        "2023-09-08 14:45:58,956 - INFO - Success, records harvested since {latest_harvest_timestamp}\n"
    ]
    with open(default_test_log_file_path, "w") as file_:
        file_.writelines(log_file_data)
    return default_test_log_file_path


@pytest.fixture
def create_test_log_file_with_unsuccessful_harvest(default_test_log_file_path):
    """Create a temporary log file with no successfull harvests and clean up afterwards."""
    log_file_data = ["2023-09-08 14:34:16,887 - INFO - Started\n"]
    with open(default_test_log_file_path, "w") as file_:
        file_.writelines(log_file_data)
    return default_test_log_file_path


def test_last_harvest_date(create_test_log_file):
    """Test getting the last start time of successful harvest date in the log file"""
    harvested_date = metadata_harvester_cli.last_harvest_date(create_test_log_file)
    assert harvested_date == "2023-09-08T14:45:58Z"


def test_get_last_harvest_no_file():
    """Test handling non-existing file."""
    harvested_date = metadata_harvester_cli.last_harvest_date("harvester_test.log")
    assert harvested_date is None


@pytest.fixture
def create_test_log_file_with_one_successful_harvest(default_test_log_file_path):
    """Create a temporary log file for testing and clean up afterwards."""
    # log_file = "harvester_test.log"
    log_file_data = [
        "2023-09-08 14:34:16,887 - INFO - Started\n"
        "2023-09-08 14:42:58,652 - INFO - Success, all records harvested\n"
        "2023-09-08 14:44:58,690 - INFO - Started\n"
        "2023-09-08 14:45:58,690 - INFO - Started\n"
        "2023-09-08 14:45:58,956 - INFO - Started\n"
    ]
    with open(default_test_log_file_path, "w") as file_:
        file_.writelines(log_file_data)
    return default_test_log_file_path


def test_get_last_harvest_with_unsuccessful_harvest(
    create_test_log_file_with_one_successful_harvest,
):
    """Test getting the last start time of successful harvest date in the log file"""
    harvested_date = metadata_harvester_cli.last_harvest_date(
        create_test_log_file_with_one_successful_harvest
    )
    assert harvested_date == "2023-09-08T14:34:16Z"


@pytest.mark.usefixtures(
    "mock_cmdi_record_not_found_in_datacatalog",
    "mock_single_pid_list_from_cmdi",
    "mock_pids_in_datacatalog_matching_cmdi",
    "create_test_log_file_with_unsuccessful_harvest",
)
def test_full_harvest_all_data_harvested_and_records_in_sync(
    mock_requests_post,
    mock_list_records_single_record,
    caplog,
    run_cli,
):
    """
    Check that when no successful harvest date is available (the log file does not have any successful harvests logged), all data is fetched from Kielipankki.

    The test also covers the situation where none of the record PID matches the ones in Metax so the data is POSTed to Metax.

    Finally, the records from both services are compared and no diffs are found so they are in sync.
    """
    with caplog.at_level(logging.INFO):
        result = run_cli(metadata_harvester_cli.full_harvest)

    assert result.exit_code == 0

    assert mock_requests_post.call_count == 6
    assert mock_requests_post.request_history[3].method == "POST"
    assert (
        mock_requests_post.request_history[3].json()["persistent_identifier"]
        == mock_list_records_single_record[0]["persistent_identifier"]
    )

    assert "Success, 1 records harvested" in caplog.text
    assert "faulty" not in caplog.text


@pytest.mark.usefixtures(
    "mock_single_pid_list_from_cmdi",
    "mock_pids_in_datacatalog",
    "mock_cmdi_record_found_in_datacatalog",
    "mock_delete_record",
    "create_test_log_file_with_unsuccessful_harvest",
)
def test_full_harvest_all_data_harvested_and_records_not_in_sync(
    mock_requests_put,
    mock_list_records_single_record,
    caplog,
    run_cli,
):
    """
    Check that when no successful harvest date is available (the log file does not have
    any successful harvests logged), all data is fetched from Kielipankki.

    The test also covers the situation where the record PID matches the ones in Metax so
    the data is PUT to Metax.

    Finally, the records from both services are compared resulting in one non-matching
    PID. This non-matching record is then DELETEd from Metax.

    Expected requests made during this test:
    GET to fetch the records from Comedi (for adding new records)
    GET to get the Metax PID (found)
    GET to determine which language codes are accepted by Metax
    PUT to to update the data in Metax
    GET to fetch the records from Comedi (again, for deleted records this time)
    GET to fetch the records from Metax (no overlap, so no further requests)
    """
    with caplog.at_level(logging.INFO):
        result = run_cli(metadata_harvester_cli.full_harvest)

    assert result.exit_code == 0

    assert mock_requests_put.call_count == 6
    assert mock_requests_put.request_history[3].method == "PUT"
    assert (
        mock_requests_put.request_history[3].json()["persistent_identifier"]
        == mock_list_records_single_record[0]["persistent_identifier"]
    )

    assert "Success, 1 records harvested" in caplog.text
    assert "faulty" not in caplog.text


@pytest.mark.usefixtures(
    "mock_single_pid_list_from_cmdi",
    "mock_pids_in_datacatalog_matching_cmdi",
    "mock_delete_record",
    "mock_cmdi_record_not_found_in_datacatalog",
    "create_test_log_file",
)
def test_full_harvest_new_records_harvested_since_date_and_records_in_sync(
    mock_requests_post,
    mock_list_records_single_record,
    caplog,
    run_cli,
):
    """
    Check that, when there is a successful harvest logged, new and updated records since that
    date are fetched from Kielipankki and then sent to Metax.

    This test covers POSTing new records.

    Finally, the records from both services are compared and no diffs are found so they are in sync.
    """
    with caplog.at_level(logging.INFO):
        result = run_cli(metadata_harvester_cli.full_harvest)

    assert result.exit_code == 0

    assert mock_requests_post.call_count == 6
    assert mock_requests_post.request_history[3].method == "POST"
    assert (
        mock_requests_post.request_history[3].json()["persistent_identifier"]
        == mock_list_records_single_record[0]["persistent_identifier"]
    )

    assert "Success, 1 records harvested since" in caplog.text
    assert "faulty" not in caplog.text


@pytest.mark.usefixtures(
    "mock_cmdi_record_found_in_datacatalog",
    "mock_single_pid_list_from_cmdi",
    "mock_pids_in_datacatalog",
    "mock_delete_record",
    "create_test_log_file",
)
def test_full_harvest_changed_records_harvested_since_date_and_records_not_in_sync_with_delete(
    mock_requests_put,
    mock_list_records_single_record,
    caplog,
    run_cli,
):
    """
    Check that, when there is a successful harvest logged previously, new and updated
    records are fetched from Kielipankki and then sent to Metax.

    This test covers only changed records that are PUT to Metax.

    Finally, the records from both services are compared resulting in one non-matching
    PID. This non-matching record is then DELETEd from Metax. As it was already deleted,
    it should not be listed in the output to be emailed to inform admins of required
    manual actions.

    Expected requests made during this test:
    GET to fetch the records from Comedi (for adding new records)
    GET to get the Metax PID (found)
    PUT to to update the data in Metax
    GET to fetch the records from Comedi (again, for deleted records this time)
    GET to fetch the records from Metax
    GET to get the Metax PID for the record to be deleted
    DELETE to delete the record
    """
    with caplog.at_level(logging.INFO):
        result = run_cli(
            metadata_harvester_cli.full_harvest, extra_args=["--automatic-delete"]
        )

    assert result.exit_code == 0

    assert mock_requests_put.call_count == 8
    assert mock_requests_put.request_history[3].method == "PUT"
    assert (
        mock_requests_put.request_history[3].json()["persistent_identifier"]
        == mock_list_records_single_record[0]["persistent_identifier"]
    )

    assert "Success, 1 records harvested since" in caplog.text
    assert "faulty" not in caplog.text

    assert "pid2" not in result.stderr


@pytest.mark.usefixtures(
    "mock_cmdi_record_found_in_datacatalog",
    "mock_single_pid_list_from_cmdi",
    "mock_pids_in_datacatalog",
    "mock_delete_record",
    "create_test_log_file",
)
def test_full_harvest_changed_records_harvested_since_date_and_records_not_in_sync(
    mock_requests_put,
    mock_list_records_single_record,
    caplog,
    run_cli,
):
    """
    Check that, when there is a successful harvest logged previously, new and updated
    records are fetched from Kielipankki and then sent to Metax.

    This test covers only changed records that are PUT to Metax.

    Finally, the records from both services are compared resulting in one non-matching
    PID. This non-matching record is not deleted from Metax, but is listed in stderr
    output.

    Expected requests made during this test:
    GET to fetch the records from Comedi (for adding new records)
    GET to get the Metax PID (found)
    PUT to to update the data in Metax
    GET to fetch the records from Comedi (again, for deleted records this time)
    GET to fetch the records from Metax (for comparing with Comedi)
    """
    with caplog.at_level(logging.INFO):
        result = run_cli(metadata_harvester_cli.full_harvest)

    assert result.exit_code == 0

    assert mock_requests_put.call_count == 6
    assert mock_requests_put.request_history[3].method == "PUT"
    assert (
        mock_requests_put.request_history[3].json()["persistent_identifier"]
        == mock_list_records_single_record[0]["persistent_identifier"]
    )

    assert "Success, 1 records harvested since" in caplog.text
    assert "faulty" not in caplog.text

    assert "pid2" in result.stderr


@pytest.mark.usefixtures(
    "mock_cmdi_get_multiple_records",
    "mock_requests_post",
    "mock_cmdi_record_not_found_in_datacatalog",
    "create_test_log_file_with_unsuccessful_harvest",
)
def test_full_harvest_multiple_records(
    shared_request_mocker,
    run_cli,
):
    """
    Check that multiple records are looped over properly.

    This is verified by checking that there is one POST request for each corpus in
    Comedi and no unexpected requests happened.

    The requests we expect to see:
    GET to fetch the records from Comedi (for adding new records)
    For each corpus record (4 in test data, one record is not a corpus):
        GET to get Metax PID (not found)
        POST to send the data to Metax
    GET to fetch the records from Comedi (again, for deleted records this time)
    GET to fetch the Metax PIDs for comparison (nothing to be deleted found)
    """
    result = run_cli(metadata_harvester_cli.full_harvest)

    assert result.exit_code == 0

    assert shared_request_mocker.call_count == 12
    assert (
        sum(
            request.method == "POST"
            for request in shared_request_mocker.request_history
        )
        == 4
    )


@pytest.mark.usefixtures(
    "mock_cmdi_get_multiple_records",
    "mock_requests_post",
    "mock_cmdi_record_not_found_in_datacatalog",
)
def test_full_harvest_without_log_file(shared_request_mocker, run_cli):
    """
    Check that full harvest is done when a log file doesn't exist.

    The log file will not exist because the tests use a log file in pytest-created
    temporary file that is per-test, and this test does not use a fixture that would
    create one.

    This is verified by checking that there is one POST request for each corpus in
    Comedi and no unexpected requests happened.

    The requests we expect to see:
    GET to fetch the records from Comedi (for adding new records)
    For each corpus record (4 in test data):
        GET to get Metax PID (not found)
        POST to send the data to Metax
    GET to fetch the records from Comedi (again, for deleted records this time)
    GET to fetch the Metax PIDs for comparison (nothing to be deleted found)
    """
    result = run_cli(metadata_harvester_cli.full_harvest)

    assert result.exit_code == 0

    assert shared_request_mocker.call_count == 12
    assert (
        sum(
            request.method == "POST"
            for request in shared_request_mocker.request_history
        )
        == 4
    )


@pytest.mark.usefixtures(
    "mock_cmdi_get_no_new_records",
    "mock_cmdi_record_not_found_in_datacatalog",
)
def test_full_harvest_no_new_records(
    shared_request_mocker,
    run_cli,
):
    """
    Test that when there are no new records, the program moves directly to checking for
    records pending removal


    The requests we expect to see:
    GET to fetch the records from Comedi (for adding new records, none found)
    GET to fetch the records from Comedi (again, for deleted records this time, none
        found)
    GET to fetch the Metax PIDs for comparison (none found)
    """
    run_cli(metadata_harvester_cli.full_harvest)

    assert shared_request_mocker.call_count == 3
    assert all(r.method == "GET" for r in shared_request_mocker.request_history)


def test_cli_reporting_missing_configuration_values(create_test_config_file, run_cli):
    """
    Test that not providing all required configuration values in config file will
    produce an informative error message and non-zero exit code.
    """
    create_test_config_file(
        {
            "metax_api_token": "qwerty",
            "metax_base_url": "https://metax.demo.fairdata.fi/",
            "metax_catalog_id": "abc123",
            # harvester_log_file not defined
            "metax_api_log_file": "log.txt",
            "save_records_locally": False,
        }
    )
    result = run_cli(metadata_harvester_cli.full_harvest)
    assert 'Value for "harvester_log_file" not found in configuration file'
    assert result.exit_code != 0


@pytest.mark.usefixtures(
    "mock_requests_post",
    "mock_cmdi_record_not_found_in_datacatalog",
    "mock_list_records_no_in_progress",
)
def test_cli_backing_up_records(
    create_test_config_file,
    run_cli,
    tmp_path,
    default_test_log_file_path,
    default_metax_api_log_file_path,
    mock_list_records_single_record,
):
    """
    Test backing records up locally.

    The test is set up so, that there should be one published record and no in progress
    records.

    We are not interested in any of the requests made, we just want to verify that:
    - the published and in progress directories are created
    - there are no items or subdirectories in the in progress directory
    - there is one item in the published directory
    - the item in the published directory is a file with the expected file name
    - the file is not empty
    """
    backup_dir = tmp_path / "test-backups"
    create_test_config_file(
        {
            "metax_api_token": "qwerty",
            "metax_base_url": "https://metax.demo.fairdata.fi/v3",
            "metax_catalog_id": "abc123",
            "harvester_log_file": str(default_test_log_file_path),
            "metax_api_log_file": str(default_metax_api_log_file_path),
            "save_records_locally": True,
            "save_destination_directory": str(backup_dir),
        }
    )
    result = run_cli(metadata_harvester_cli.full_harvest)
    assert result.exit_code == 0

    record_identifier = mock_list_records_single_record[0][
        "persistent_identifier"
    ].split(":")[-1]

    backup_dir_contents = list(backup_dir.iterdir())
    assert len(backup_dir_contents) == 2

    in_progress_dir = backup_dir / "in_progress"
    assert in_progress_dir in backup_dir_contents
    assert len(list(in_progress_dir.iterdir())) == 0

    published_dir = backup_dir / "published"
    assert published_dir in backup_dir_contents
    assert len(list(published_dir.iterdir())) == 1

    expected_backup_file = published_dir / f"{record_identifier}.xml"
    assert expected_backup_file.exists()
    assert expected_backup_file.is_file()
    assert os.path.getsize(expected_backup_file) > 0


@pytest.mark.usefixtures(
    "mock_cmdi_get_no_new_records",
    "mock_cmdi_record_not_found_in_datacatalog",
)
def test_cli_backing_up_in_progress_records(
    create_test_config_file,
    run_cli,
    tmp_path,
    default_test_log_file_path,
    default_metax_api_log_file_path,
    mock_list_records_single_record_in_progress,
):
    """
    Test backing records up locally.

    The test is set up so, that there should be one in progress record and no published
    records.

    We are not interested in any of the requests made, we just want to verify that:
    - the published and in progress directories are created
    - there is one item in the in progress directory
    - the item in the in progress directory is a file with the expected file name
    - the file is not empty
    - there are no items or subdirectories in the in published directory
    """
    backup_dir = tmp_path / "test-backups"
    create_test_config_file(
        {
            "metax_api_token": "qwerty",
            "metax_base_url": "https://metax.demo.fairdata.fi/v3",
            "metax_catalog_id": "abc123",
            "harvester_log_file": str(default_test_log_file_path),
            "metax_api_log_file": str(default_metax_api_log_file_path),
            "save_records_locally": True,
            "save_destination_directory": str(backup_dir),
        }
    )
    result = run_cli(metadata_harvester_cli.full_harvest)
    assert result.exit_code == 0

    record_identifier = mock_list_records_single_record_in_progress[0][
        "persistent_identifier"
    ].split(":")[-1]

    backup_dir_contents = list(backup_dir.iterdir())
    assert len(backup_dir_contents) == 2

    in_progress_dir = backup_dir / "in_progress"
    assert in_progress_dir in backup_dir_contents
    assert len(list(in_progress_dir.iterdir())) == 1

    expected_backup_file = in_progress_dir / f"{record_identifier}.xml"
    assert expected_backup_file.exists()
    assert expected_backup_file.is_file()
    assert os.path.getsize(expected_backup_file) > 0

    published_dir = backup_dir / "published"
    assert published_dir in backup_dir_contents
    assert len(list(published_dir.iterdir())) == 0

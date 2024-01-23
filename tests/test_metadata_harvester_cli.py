import logging

from click.testing import CliRunner
import pytest
import yaml

import metadata_harvester_cli
from metadata_harvester_cli import full_harvest

# Pylint does not understand fixture use
# pylint: disable=redefined-outer-name


@pytest.fixture
def default_test_log_file_path(tmp_path):
    return tmp_path / "harvester_test.log"


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
def create_test_config_file(tmp_path):
    """
    Factory helper for configuration files to be used in tests.
    """

    def _create_config(configuration_data):
        """
        Write the given configuration data into a temporary config file.

        :return: path to the newly-created config file as a string
        """
        config_filename = tmp_path / "config.yml"
        with open(config_filename, "w") as config_file:
            yaml.dump(configuration_data, stream=config_file)
        return str(config_filename)

    return _create_config


@pytest.fixture
def basic_configuration(
    create_test_config_file, default_test_log_file_path, default_metax_api_log_file_path
):
    """
    Create a basic well-formed configuration file and return its path.
    """
    return create_test_config_file(
        {
            "metax_api_token": "apitokentestvalue",
            "harvester_log_file": str(default_test_log_file_path),
            "metax_api_log_file": str(default_metax_api_log_file_path),
        }
    )


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


@pytest.fixture
def run_cli(basic_configuration):
    """
    Helper for running the command line interface with given arguments.

    If some argument is not specified, default testing values are used.

    File arguments can be specified as strings or Paths: they are automatically
    converted.
    """

    def _run_cli(configuration_file_path=None):
        if configuration_file_path is None:
            configuration_file_path = basic_configuration

        runner = CliRunner()
        return runner.invoke(
            full_harvest,
            [str(configuration_file_path)],
        )

    return _run_cli


@pytest.mark.usefixtures(
    "mock_metashare_record_not_found_in_datacatalog",
    "mock_single_pid_list_from_metashare",
    "mock_pids_in_datacatalog_matching_metashare",
    "create_test_log_file_with_unsuccessful_harvest",
)
def test_full_harvest_all_data_harvested_and_records_in_sync(
    mock_requests_post,
    mock_metashare_get_single_record,
    caplog,
    run_cli,
):
    """
    Check that when no successful harvest date is available (the log file does not have any successful harvests logged), all data is fetched from Kielipankki.

    The test also covers the situation where none of the record PID matches the ones in Metax so the data is POSTed to Metax.

    Finally, the records from both services are compared and no diffs are found so they are in sync.
    """
    with caplog.at_level(logging.INFO):
        result = run_cli()

    assert result.exit_code == 0

    assert mock_requests_post.call_count == 5
    assert mock_requests_post.request_history[2].method == "POST"
    assert (
        mock_requests_post.request_history[2].json()["persistent_identifier"]
        == mock_metashare_get_single_record[0]["persistent_identifier"]
    )

    assert "Success, all records harvested" in caplog.text


@pytest.mark.usefixtures(
    "mock_single_pid_list_from_metashare",
    "mock_pids_in_datacatalog",
    "mock_metashare_record_found_in_datacatalog",
    "mock_delete_record",
    "create_test_log_file_with_unsuccessful_harvest",
)
def test_full_harvest_all_data_harvested_and_records_not_in_sync(
    mock_requests_put,
    mock_metashare_get_single_record,
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
    GET to fetch the records from Metashare (for adding new records)
    GET to get the Metax PID (found)
    PUT to to update the data in Metax
    GET to fetch the records from Metashare (again, for deleted records this time)
    GET to fetch the records from Metax (no overlap, so no further requests)
    """
    with caplog.at_level(logging.INFO):
        result = run_cli()

    assert result.exit_code == 0

    assert mock_requests_put.call_count == 5
    assert mock_requests_put.request_history[2].method == "PUT"
    assert (
        mock_requests_put.request_history[2].json()["persistent_identifier"]
        == mock_metashare_get_single_record[0]["persistent_identifier"]
    )

    assert "Success, all records harvested" in caplog.text


@pytest.mark.usefixtures(
    "mock_single_pid_list_from_metashare",
    "mock_pids_in_datacatalog_matching_metashare",
    "mock_delete_record",
    "mock_metashare_record_not_found_in_datacatalog",
    "create_test_log_file",
)
def test_full_harvest_new_records_harvested_since_date_and_records_in_sync(
    mock_requests_post,
    mock_metashare_get_single_record,
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
        result = run_cli()

    assert result.exit_code == 0

    assert mock_requests_post.call_count == 5
    assert mock_requests_post.request_history[2].method == "POST"
    assert (
        mock_requests_post.request_history[2].json()["persistent_identifier"]
        == mock_metashare_get_single_record[0]["persistent_identifier"]
    )

    assert "Success, records harvested since" in caplog.text


@pytest.mark.usefixtures(
    "mock_metashare_record_found_in_datacatalog",
    "mock_single_pid_list_from_metashare",
    "mock_pids_in_datacatalog",
    "mock_delete_record",
    "create_test_log_file",
)
def test_full_harvest_changed_records_harvested_since_date_and_records_not_in_sync(
    mock_requests_put,
    mock_metashare_get_single_record,
    caplog,
    run_cli,
):
    """
    Check that, when there is a successful harvest logged previously, new and updated
    records are fetched from Kielipankki and then sent to Metax.

    This test covers only changed records that are PUT to Metax.

    Finally, the records from both services are compared resulting in one non-matching
    PID. This non-matching record is then DELETEd from Metax.

    Expected requests made during this test:
    GET to fetch the records from Metashare (for adding new records)
    GET to get the Metax PID (found)
    PUT to to update the data in Metax
    GET to fetch the records from Metashare (again, for deleted records this time)
    GET to fetch the records from Metax
    GET to get the Metax PID for the record to be deleted
    DELETE to delete the record
    """
    with caplog.at_level(logging.INFO):
        result = run_cli()

    assert result.exit_code == 0

    assert mock_requests_put.call_count == 7
    assert mock_requests_put.request_history[2].method == "PUT"
    assert (
        mock_requests_put.request_history[2].json()["persistent_identifier"]
        == mock_metashare_get_single_record[0]["persistent_identifier"]
    )

    assert "Success, records harvested since" in caplog.text


@pytest.mark.usefixtures(
    "mock_metashare_get_multiple_records",
    "mock_requests_post",
    "mock_metashare_record_not_found_in_datacatalog",
    "create_test_log_file_with_unsuccessful_harvest",
)
def test_full_harvest_multiple_records(
    shared_request_mocker,
    run_cli,
):
    """
    Check that multiple records are looped over properly.

    This is verified by checking that there is one POST request for each corpus in
    Metashare and no unexpected requests happened.

    The requests we expect to see:
    GET to fetch the records from Metashare (for adding new records)
    For each corpus record (5 in test data):
        GET to get Metax PID (not found)
        POST to send the data to Metax
    GET to fetch the records from Metashare (again, for deleted records this time)
    GET to fetch the Metax PIDs for comparison (nothing to be deleted found)
    """
    result = run_cli()

    assert result.exit_code == 0

    assert shared_request_mocker.call_count == 13
    assert (
        sum(
            request.method == "POST"
            for request in shared_request_mocker.request_history
        )
        == 5
    )


@pytest.mark.usefixtures(
    "mock_metashare_get_multiple_records",
    "mock_requests_post",
    "mock_metashare_record_not_found_in_datacatalog",
)
def test_full_harvest_without_log_file(shared_request_mocker, run_cli):
    """
    Check that full harvest is done when a log file doesn't exist.

    The log file will not exist because the tests use a log file in pytest-created
    temporary file that is per-test, and this test does not use a fixture that would
    create one.

    This is verified by checking that there is one POST request for each corpus in
    Metashare and no unexpected requests happened.

    The requests we expect to see:
    GET to fetch the records from Metashare (for adding new records)
    For each corpus record (5 in test data):
        GET to get Metax PID (not found)
        POST to send the data to Metax
    GET to fetch the records from Metashare (again, for deleted records this time)
    GET to fetch the Metax PIDs for comparison (nothing to be deleted found)
    """
    result = run_cli()

    assert result.exit_code == 0

    assert shared_request_mocker.call_count == 13
    assert (
        sum(
            request.method == "POST"
            for request in shared_request_mocker.request_history
        )
        == 5
    )


@pytest.mark.usefixtures(
    "mock_metashare_get_no_new_records",
    "mock_metashare_record_not_found_in_datacatalog",
)
def test_full_harvest_no_new_records(
    shared_request_mocker,
    run_cli,
):
    """
    Test that when there are no new records, the program moves directly to checking for
    records pending removal


    The requests we expect to see:
    GET to fetch the records from Metashare (for adding new records, none found)
    GET to fetch the records from Metashare (again, for deleted records this time, none
        found)
    GET to fetch the Metax PIDs for comparison (none found)
    """
    run_cli()

    assert shared_request_mocker.call_count == 3
    assert all(r.method == "GET" for r in shared_request_mocker.request_history)

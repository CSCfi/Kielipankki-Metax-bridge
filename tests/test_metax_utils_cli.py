import logging

from click.testing import CliRunner
import pytest

import metax_utils_cli


@pytest.mark.usefixtures(
    "mock_cmdi_record_found_in_datacatalog",
    "mock_requests_get_record",
)
def test_delete_record(
    metax_dataset_id,
    mock_delete_record,
    dataset_pid,
    run_cli,
):
    """
    Test deleting a single record from Metax successfully.
    """
    result = run_cli(metax_utils_cli.delete_record, extra_args=[dataset_pid])

    assert result.exit_code == 0

    assert mock_delete_record.call_count == 2  # GET and DELETE both shown here
    assert mock_delete_record.request_history[0].method == "GET"
    assert mock_delete_record.request_history[1].method == "DELETE"
    assert metax_dataset_id in mock_delete_record.request_history[1].path

    assert f"Deleting record {dataset_pid}" in result.stdout
    assert f"{metax_dataset_id}" in result.stdout
    assert "Record deleted" in result.stdout


@pytest.mark.usefixtures(
    "mock_cmdi_record_not_found_in_datacatalog",
)
def test_delete_nonexistent_record(
    mock_delete_record,
    dataset_pid,
    run_cli,
):
    """
    Test deleting a single record from Metax successfully.
    """
    result = run_cli(metax_utils_cli.delete_record, extra_args=[dataset_pid])

    assert result.exit_code == 0

    assert mock_delete_record.call_count == 1
    assert mock_delete_record.request_history[0].method == "GET"

    assert f"Record {dataset_pid} not found in Metax" in result.stdout

import logging
import pytest
import requests
import requests_mock

from metax_api import MetaxAPI


def test_api_token_in_headers(mock_requests_post, default_metax_api_log_file_path):
    """
    Verify that the given API token is used when making requests.
    """
    metax = MetaxAPI("token_test_value", str(default_metax_api_log_file_path))
    metax.create_record({"dummy": "data"})
    assert (
        mock_requests_post.request_history[0].headers["Authorization"]
        == "Token token_test_value"
    )


@pytest.mark.usefixtures("mock_requests_post")
def test_api_calls_logged(metax_api, default_metax_api_log_file_path):
    """
    Check that configuring the Metax API call logging works.

    This is verified by ensuring that initially empty log file gets content when an API
    call is made.
    """
    assert default_metax_api_log_file_path.stat().st_size == 0
    metax_api.create_record({"dummy": "data"})
    assert default_metax_api_log_file_path.stat().st_size != 0


def test_record_id_pid_in_datacatalog(
    dataset_pid, mock_requests_get_record, metax_api, mock_get_response_json
):
    """Check that a dataset with a given PID returns an id from Metax by making a GET request."""
    result = metax_api.record_id(dataset_pid)
    assert result == mock_get_response_json["results"][0]["id"]
    assert mock_requests_get_record.call_count == 1


def test_record_id_pid_not_in_datacatalog(
    mock_requests_get_record, metax_base_url, kielipankki_datacatalog_id, metax_api
):
    """Test that a non-existing PID in Metax returns None"""
    dataset_pid = "urn.fi//urn:nbn:fi:lb-0000000"
    mock_requests_get_record.get(
        f"{metax_base_url}/datasets?data_catalog__id={kielipankki_datacatalog_id}&persistent_identifier={dataset_pid}",
        json={"count": 0},
    )
    result = metax_api.record_id(dataset_pid)
    assert not result
    assert mock_requests_get_record.call_count == 1


def test_create_record_successful(
    mock_requests_post, caplog, metax_api, mock_post_put_response_json
):
    """Check that a successful post request to Metax is made of a well-formed dictionary."""
    metadata_dict = {
        "persistent_identifier": "urn.fi/urn:nbn:fi:lb-201603170300",
        "title": {"en": "The Corpus"},
        "description": {"en": "A large corpus"},
        "modified": "2016-03-17T00:00:00.000000Z",
        "issued": "2016-03-17T00:00:00.000000Z",
        "access_rights": {
            "license": [
                {
                    "url": "http://uri.suomi.fi/codelist/fairdata/license/code/undernegotiation"
                }
            ],
            "access_type": {
                "url": "http://uri.suomi.fi/codelist/fairdata/access_type/code/restricted"
            },
        },
    }

    with caplog.at_level(logging.INFO):
        response_json = metax_api.create_record(metadata_dict)
    assert response_json == mock_post_put_response_json
    assert "Request succeeded. Method: POST" in caplog.text
    assert mock_requests_post.call_count == 1
    assert mock_requests_post.request_history[0].method == "POST"


def test_create_dataset_failed(mock_requests_post, caplog, metax_base_url, metax_api):
    """Check that an ill-formed dictionary results in a bad request to Metax."""
    mock_requests_post.post(f"{metax_base_url}/datasets", status_code=400)
    metadata_dict = {
            "invalid_key": "invalid_value"
            }

    with pytest.raises(requests.exceptions.RequestException), caplog.at_level(
        logging.ERROR
    ):
        response = metax_api.create_record(metadata_dict)
        assert response.status_code == 400
    assert mock_requests_post.request_history[0].method == "POST"
    assert mock_requests_post.call_count == 1
    assert "Request failed. Method: POST" in caplog.text


def test_update_dataset_successful(
    mock_requests_put, caplog, metax_api, mock_post_put_response_json
):
    """Test that an existing dataset in Metax is successfully updated."""
    metax_dataset_id = "1f32f478-8e7e-4d72-9638-d29a4f1430aa"
    sample_dict = {
        "persistent_identifier": "urn.fi/urn:nbn:fi:lb-201603170300",
        "title": {"en": "The Corpus"},
        "description": {"en": "A large corpus"},
        "modified": "2016-03-17T00:00:00.000000Z",
        "issued": "2016-03-17T00:00:00.000000Z",
        "access_rights": {
            "license": [
                {
                    "url": "http://uri.suomi.fi/codelist/fairdata/license/code/undernegotiation"
                }
            ],
            "access_type": {
                "url": "http://uri.suomi.fi/codelist/fairdata/access_type/code/restricted"
            },
        },
    }

    with caplog.at_level(logging.INFO):
        response = metax_api.update_record(metax_dataset_id, sample_dict)
    assert response == mock_post_put_response_json
    assert "Request succeeded. Method: PUT" in caplog.text
    assert mock_requests_put.call_count == 1
    assert mock_requests_put.request_history[0].method == "PUT"


def test_update_dataset_failed(mock_requests_put, caplog, metax_base_url, metax_api):
    """Test that an ill-formed dictionary results in a bad request to Metax."""
    metax_dataset_id = "441560f5-4c2a-48eb-bc1a-489639ec3573"
    mock_requests_put.put(
        f"{metax_base_url}/datasets/{metax_dataset_id}", status_code=400
    )
    sample_dict = {
        "persistent_identifier": "urn.fi/urn:nbn:fi:lb-201603170300",
        "title": {"en": "The Corpus"},
        "description": {"en": "A large corpus"},
        "modified": "2016-03-17T00:00:00.000000Z",
        "issued": "2016-03-17T00:00:00.000000Z",
        "access_rights": {
            "license": [
                {
                    "url": "http://uri.suomi.fi/codelist/fairdata/license/code/undernegotiation"
                }
            ],
            "access_type": {
                "orl": "http://uri.suomi.fi/codelist/fairdata/access_type/code/restricted"
            },
        },
    }

    with pytest.raises(requests.exceptions.HTTPError), caplog.at_level(logging.ERROR):
        response = metax_api.update_record(metax_dataset_id, sample_dict)
        assert response.status_code == 400
    assert "Request failed. Method: PUT" in caplog.text
    assert mock_requests_put.call_count == 1
    assert mock_requests_put.request_history[0].method == "PUT"


def test_delete_record(mock_delete_record, metax_dataset_id, metax_api, metax_base_url):
    """
    Test that a DELETE request of a dataset is successful (204 response).
    """
    mock_delete_record.delete(
        f"{metax_base_url}/datasets/{metax_dataset_id}",
        status_code=204,
    )
    response = metax_api.delete_record(metax_dataset_id)
    assert mock_delete_record.call_count == 1
    assert mock_delete_record.request_history[0].method == "DELETE"
    assert response.status_code == 204


def test_delete_record_failed(
    mock_delete_record, metax_dataset_id, metax_api, caplog, metax_base_url
):
    """
    Test that a DELETE request of a dataset fails (404 response).
    """
    mock_delete_record.delete(
        f"{metax_base_url}/datasets/{metax_dataset_id}",
        status_code=404,
    )
    with pytest.raises(requests.exceptions.HTTPError), caplog.at_level(logging.ERROR):
        response = metax_api.delete_record(metax_dataset_id)
        assert response.status_code == 404
    assert mock_delete_record.call_count == 1
    assert mock_delete_record.request_history[0].method == "DELETE"
    assert "Request failed. Method: DELETE" in caplog.text


def test_datacatalog_dataset_record_pids(mock_pids_in_datacatalog, metax_api):
    """
    Test that querying a datacatalog in metax returns a set of all  its PIDs.
    """
    assert metax_api.datacatalog_record_pids == mock_pids_in_datacatalog


@pytest.mark.usefixtures(
    "mock_metashare_record_not_found_in_datacatalog",
    "mock_requests_post",
)
def test_send_record(
    shared_request_mocker,
    metax_api,
    basic_metashare_record,
):
    """
    Check that creating one new metadata record works

    This means that Metax is queried for existence of the PID (not found) and then a new
    record is POSTed. We also check that the posted data corresponds to the record dict
    passed to the function.
    """
    metax_api.send_record(basic_metashare_record)

    assert shared_request_mocker.call_count == 2

    expected_post_request = shared_request_mocker.request_history[1]

    assert expected_post_request.method == "POST"
    assert expected_post_request.json() == basic_metashare_record.to_dict()


@pytest.mark.usefixtures(
    "mock_metashare_record_found_in_datacatalog",
    "mock_requests_put",
)
def test_send_data_to_metax_single_pre_existing_record(
    shared_request_mocker,
    metax_api,
    basic_metashare_record,
):
    """
    Check that creating one new metadata record works

    This means that Metax is queried for existence of the PID (found), fetches the Metax
    ID for the record, and then a new record is PUT. We also check that the posted
    data corresponds to the record dict passed to the function.
    """
    metax_api.send_record(basic_metashare_record)

    assert shared_request_mocker.call_count == 2

    expected_put_request = shared_request_mocker.request_history[1]

    assert expected_put_request.method == "PUT"
    assert expected_put_request.json() == basic_metashare_record.to_dict()


@pytest.mark.usefixtures(
    "mock_metashare_record_found_in_datacatalog",
    "mock_pids_in_datacatalog",
)
def test_delete_records_not_in_smaller_set(
    mock_delete_record, metax_api, basic_metashare_record
):
    """
    Check that when there is one record to be removed, it is removed.

    This is checked by verifying that there is exactly one DELETE request.

    We expect to see the following requests:
    GET to fetch the records from Metax
    GET to determine the Metax ID for the one record to be removed
    DELETE to actually delete the record based on its Metax ID
    """
    metax_api.delete_records_not_in([basic_metashare_record])
    assert mock_delete_record.call_count == 3
    assert (
        sum(
            request.method == "DELETE" for request in mock_delete_record.request_history
        )
        == 1
    )


@pytest.mark.usefixtures(
    "mock_pids_in_datacatalog_matching_metashare",
)
def test_delete_records_not_in_equal_set(
    mock_delete_record, metax_api, basic_metashare_record
):
    """
    Check that no records are removed when all should still be present.

    This means that we expect to see only one request (the GET for records in
    Metashare), and specifically no DELETE requests.
    """
    metax_api.delete_records_not_in([basic_metashare_record])
    assert mock_delete_record.call_count == 1
    assert mock_delete_record.request_history[0].method != "DELETE"


@pytest.mark.usefixtures(
    "mock_pids_in_datacatalog_matching_metashare",
)
def test_delete_records_larger_set(
    mock_delete_record,
    metax_api,
    basic_metashare_record,
    license_with_custom_url_record,
):
    """
    Check that having extra records in the input set will not cause removals.

    This situation should not occur in normal use via the CLI, but if the method is
    called with extra records for any reason, we want to be sure that it won't result in
    unwanted removals.
    """
    metax_api.delete_records_not_in(
        [basic_metashare_record, license_with_custom_url_record]
    )
    assert mock_delete_record.call_count == 1
    assert mock_delete_record.request_history[0].method != "DELETE"

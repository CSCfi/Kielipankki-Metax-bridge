import logging
import pytest
import requests
import requests_mock
import metax_api


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
        mock_requests_post,
        caplog, metax_api,
        mock_post_put_response_json):
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
    mock_requests_post.post(
        f"{metax_base_url}/datasets", status_code=400)
    metadata_dict = {
        "invalid_key": "invalid_value"
    }

    with pytest.raises(requests.exceptions.RequestException), caplog.at_level(logging.ERROR):
        response = metax_api.create_record(metadata_dict)
        assert response.status_code == 400
    assert mock_requests_post.request_history[0].method == "POST"
    assert mock_requests_post.call_count == 1
    assert "Request failed. Method: POST" in caplog.text


def test_update_dataset_successful(mock_requests_put, caplog, metax_api, mock_post_put_response_json):
    """Test that an existing dataset in Metax is successfully updated."""
    metax_dataset_id = "441560f5-4c2a-48eb-bc1a-489639ec3573"
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
    mock_delete_record,
        metax_dataset_id,
        metax_api,
        caplog,
        metax_base_url):
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


def test_datacatalog_dataset_record_pids(mock_pids_list_in_datacatalog, metax_api):
    """
    Test that querying a datacatalog in metax returns a list of all  its PIDs.
    """
    result = metax_api.datacatalog_record_pids()
    assert result == mock_pids_list_in_datacatalog

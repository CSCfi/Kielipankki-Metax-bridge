import logging
import pytest
import requests
import requests_mock
import metax_api


def test_check_if_dataset_record_pid_in_datacatalog(
    dataset_pid, mock_requests_get_record, metax_base_url, kielipankki_datacatalog_id
):
    """Check that a dataset with a given PID exists in Metax by making a proper GET request to
    specified url."""
    result = metax_api.check_if_dataset_record_in_datacatalog(dataset_pid)
    assert mock_requests_get_record.call_count == 1
    assert mock_requests_get_record.request_history[0].method == "GET"
    assert result


def test_check_if_dataset_record_pid_not_in_datacatalog(
    mock_requests_get_record, metax_base_url, kielipankki_datacatalog_id
):
    """Test that a nn-existing PID in Metax is handled as expected."""
    dataset_pid = "urn.fi//urn:nbn:fi:lb-0000000"
    mock_requests_get_record.get(
        f"{metax_base_url}/datasets?data_catalog__id={kielipankki_datacatalog_id}&persistent_identifier={dataset_pid}",
        text='{"count": 0}',
    )
    result = metax_api.check_if_dataset_record_in_datacatalog(dataset_pid)
    assert not result
    assert mock_requests_get_record.call_count == 1
    assert mock_requests_get_record.request_history[0].method == "GET"


def test_get_dataset_record_metax_id(dataset_pid, mock_requests_get_record):
    """Test that a Metax record is returned for an existing PID."""
    result = metax_api.get_dataset_record_metax_id(dataset_pid)
    assert result == "1f32f478-8e7e-4d72-9638-d29a4f1430aa"
    assert mock_requests_get_record.call_count == 1
    assert mock_requests_get_record.request_history[0].method == "GET"


def test_create_dataset_successful(mock_requests_post, caplog):
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
        response_text = metax_api.create_dataset(metadata_dict)
    assert response_text in caplog.text
    assert "Created dataset" in caplog.text
    assert mock_requests_post.call_count == 1
    assert mock_requests_post.request_history[0].method == "POST"


def test_create_dataset_failed(mock_requests_post, caplog, metax_base_url):
    """Check that an ill-formed dictionary results in a bad request to Metax."""
    mock_requests_post.post(f"{metax_base_url}/datasets", status_code=400)
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
                "orl": "http://uri.suomi.fi/codelist/fairdata/access_type/code/restricted"
            },
        },
    }

    with pytest.raises(requests.exceptions.HTTPError), caplog.at_level(logging.ERROR):
        metax_api.create_dataset(metadata_dict)
    assert "Failed to create dataset" in caplog.text
    assert mock_requests_post.call_count == 1
    assert mock_requests_post.request_history[0].method == "POST"


def test_update_dataset_successful(mock_requests_put, caplog):
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
        response_text = metax_api.update_dataset(metax_dataset_id, sample_dict)
    assert response_text in caplog.text
    assert "Updated dataset" in caplog.text
    assert mock_requests_put.call_count == 1
    assert mock_requests_put.request_history[0].method == "PUT"


def test_update_dataset_failed(mock_requests_put, caplog, metax_base_url):
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
        metax_api.update_dataset(metax_dataset_id, sample_dict)
    assert "Failed to update" in caplog.text
    assert mock_requests_put.call_count == 1
    assert mock_requests_put.request_history[0].method == "PUT"

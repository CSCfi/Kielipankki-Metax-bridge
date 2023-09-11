import logging
import pytest
import requests
import requests_mock
import metax_api

@pytest.fixture
def metax_base_url():
    """Metax API"""
    return "https://metax-service.fd-staging.csc.fi/v3"

@pytest.fixture
def kielipankki_datacatalog_id():
    """Data catalog identifier for Kielipankki data."""
    return "urn:nbn:fi:att:data-catalog-kielipankki-v3"

def _get_file_as_string(filename):
    """Return given file as string."""
    with open(filename) as infile:
        return infile.read()

@pytest.fixture
def dataset_pid():
    """Return PID of sample record."""
    return "urn.fi/urn:nbn:fi:lb-2016101210"

@pytest.fixture
def mock_get_response_json():
    """A mocked response for a single dataset from Metax"""
    return _get_file_as_string("tests/test_data/metax_single_record_response.json")

@pytest.fixture
def mock_requests_get_record(mock_get_response_json, metax_base_url, kielipankki_datacatalog_id, dataset_pid):
    """A mock GET request to Metax."""
    with requests_mock.Mocker() as mocker:
        mocker.get(f"{metax_base_url}/datasets?data_catalog_id={kielipankki_datacatalog_id}&persistent_identifier={dataset_pid}", text=mock_get_response_json)
        yield mocker

def test_check_if_dataset_record_pid_in_datacatalog(dataset_pid, mock_requests_get_record, metax_base_url, kielipankki_datacatalog_id):
    """Check that a dataset with a given PID exists in Metax by making a proper GET request to specified url."""
    result = metax_api.check_if_dataset_record_in_datacatalog(dataset_pid)
    assert len(mock_requests_get_record.request_history) == 1
    assert mock_requests_get_record.request_history[0].method == "GET"
    assert mock_requests_get_record.request_history[0].url == f"{metax_base_url}/datasets?data_catalog_id={kielipankki_datacatalog_id}&persistent_identifier={dataset_pid}"
    assert result

def test_check_if_dataset_record_pid_not_in_datacatalog(mock_requests_get_record, metax_base_url, kielipankki_datacatalog_id):
    """Test that a nn-existing PID in Metax is handled as expected."""
    dataset_pid = "urn.fi//urn:nbn:fi:lb-0000000"
    mock_requests_get_record.get(f"{metax_base_url}/datasets?data_catalog_id={kielipankki_datacatalog_id}&persistent_identifier={dataset_pid}", text='{"count": 0}')
    result = metax_api.check_if_dataset_record_in_datacatalog(dataset_pid)
    assert not result
    assert len(mock_requests_get_record.request_history) == 1
    assert mock_requests_get_record.request_history[0].method == "GET"
    assert mock_requests_get_record.request_history[0].url == "https://metax-service.fd-staging.csc.fi/v3/datasets?data_catalog_id=urn:nbn:fi:att:data-catalog-kielipankki-v3&persistent_identifier=urn.fi//urn:nbn:fi:lb-0000000"


def test_get_dataset_record_metax_id(dataset_pid, mock_requests_get_record):
    """Test that a Metax record is returned for an existing PID."""
    result = metax_api.get_dataset_record_metax_id(dataset_pid)
    assert result == "1f32f478-8e7e-4d72-9638-d29a4f1430aa"
    assert len(mock_requests_get_record.request_history) == 1
    assert mock_requests_get_record.request_history[0].method == "GET"
    assert mock_requests_get_record.request_history[0].url == "https://metax-service.fd-staging.csc.fi/v3/datasets?data_catalog_id=urn:nbn:fi:att:data-catalog-kielipankki-v3&persistent_identifier=urn.fi/urn:nbn:fi:lb-2016101210"


@pytest.fixture
def mock_post_put_response_json():
    """Mock an id of a dataset in Metax."""
    return '{"id": "000-000-000"}'

@pytest.fixture
def mock_requests_post(mock_post_put_response_json, metax_base_url):
    """Mock a post request to Metax."""
    with requests_mock.Mocker() as mocker:
        mocker.post(f"{metax_base_url}/datasets", text=mock_post_put_response_json)
        yield mocker

def test_create_dataset_successful(mock_requests_post, caplog):
    """Check that a successful post request to Metax is made of a well-formed dictionary."""
    metadata_dict = {"persistent_identifier": "urn.fi/urn:nbn:fi:lb-201603170300", "title": {"en": "The Corpus"}, "description": {"en": "A large corpus"}, "modified": "2016-03-17T00:00:00.000000Z", "issued": "2016-03-17T00:00:00.000000Z", "access_rights": {"license": [{"url": "http://uri.suomi.fi/codelist/fairdata/license/code/undernegotiation"}], "access_type": {"url": "http://uri.suomi.fi/codelist/fairdata/access_type/code/restricted"}}}

    with caplog.at_level(logging.INFO):
        dataset_id = metax_api.create_dataset(metadata_dict)
    assert dataset_id == "000-000-000"
    assert "Created dataset" in caplog.text
    assert len(mock_requests_post.request_history) == 1
    assert mock_requests_post.request_history[0].method == "POST"
    assert mock_requests_post.request_history[0].url == "https://metax-service.fd-staging.csc.fi/v3/datasets"

def test_create_dataset_failed(mock_requests_post, caplog, metax_base_url):
    """Check that an ill-formed dictionary results in a bad request to Metax."""
    mock_requests_post.post(f"{metax_base_url}/datasets", status_code=400)

    metadata_dict = {"persistent_identifier": "urn.fi/urn:nbn:fi:lb-201603170300", "title": {"en": "The Corpus"}, "description": {"en": "A large corpus"}, "modified": "2016-03-17T00:00:00.000000Z", "issued": "2016-03-17T00:00:00.000000Z", "access_rights": {"license": [{"url": "http://uri.suomi.fi/codelist/fairdata/license/code/undernegotiation"}], "access_type": {"orl": "http://uri.suomi.fi/codelist/fairdata/access_type/code/restricted"}}}

    with pytest.raises(requests.exceptions.HTTPError), caplog.at_level(logging.ERROR):
        metax_api.create_dataset(metadata_dict)
    assert "Failed to create dataset" in caplog.text
    assert len(mock_requests_post.request_history) == 1
    assert mock_requests_post.request_history[0].method == "POST"
    assert mock_requests_post.request_history[0].url == "https://metax-service.fd-staging.csc.fi/v3/datasets"


@pytest.fixture
def mock_requests_put(mock_post_put_response_json, metax_base_url):
    """Mock a PUT request of a dataset to Metax."""
    metax_dataset_id = "000-000-000"
    with requests_mock.Mocker() as mocker:
        mocker.put(f"{metax_base_url}/datasets/{metax_dataset_id}", text=mock_post_put_response_json)
        yield mocker

def test_update_dataset_successful(mock_requests_put, caplog):
    """Test that an existing dataset in Metax is successfully updated."""
    metax_dataset_id = "000-000-000"
    sample_dict = {"persistent_identifier": "urn.fi/urn:nbn:fi:lb-201603170300", "title": {"en": "The Corpus"}, "description": {"en": "A large corpus"}, "modified": "2016-03-17T00:00:00.000000Z", "issued": "2016-03-17T00:00:00.000000Z", "access_rights": {"license": [{"url": "http://uri.suomi.fi/codelist/fairdata/license/code/undernegotiation"}], "access_type": {"url": "http://uri.suomi.fi/codelist/fairdata/access_type/code/restricted"}}}

    with caplog.at_level(logging.INFO):
        dataset_id = metax_api.update_dataset(metax_dataset_id, sample_dict)
    assert dataset_id == "000-000-000"
    assert "Updated dataset" in caplog.text
    assert len(mock_requests_put.request_history) == 1
    assert mock_requests_put.request_history[0].method == "PUT"
    assert mock_requests_put.request_history[0].url == "https://metax-service.fd-staging.csc.fi/v3/datasets/000-000-000"


def test_update_dataset_failed(mock_requests_put, caplog, metax_base_url):
    """Test that an ill-formed dictionary results in a bad request to Metax."""
    metax_dataset_id = "000-000-001"
    mock_requests_put.put(f"{metax_base_url}/datasets/{metax_dataset_id}", status_code=400)
    sample_dict = {"persistent_identifier": "urn.fi/urn:nbn:fi:lb-201603170300", "title": {"en": "The Corpus"}, "description": {"en": "A large corpus"}, "modified": "2016-03-17T00:00:00.000000Z", "issued": "2016-03-17T00:00:00.000000Z", "access_rights": {"license": [{"url": "http://uri.suomi.fi/codelist/fairdata/license/code/undernegotiation"}], "access_type": {"orl": "http://uri.suomi.fi/codelist/fairdata/access_type/code/restricted"}}}

    with pytest.raises(requests.exceptions.HTTPError), caplog.at_level(logging.ERROR):
        metax_api.update_dataset(metax_dataset_id, sample_dict)
    assert "Failed to update" in caplog.text
    assert len(mock_requests_put.request_history) == 1
    assert mock_requests_put.request_history[0].method == "PUT"
    assert mock_requests_put.request_history[0].url == "https://metax-service.fd-staging.csc.fi/v3/datasets/000-000-001"


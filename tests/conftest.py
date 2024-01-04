import re
import json

import pytest
import requests_mock
from metax_api import MetaxAPI


@pytest.fixture(autouse=True)
def prevent_online_http_requests(monkeypatch):
    """
    Patch urlopen so that all non-patched requests raise an error.
    """

    def urlopen_error(self, method, url, *args, **kwargs):
        raise RuntimeError(
            f"Requests are not allowed in tests, but a test attempted a "
            f"{method} request to {self.scheme}://{self.host}{url}"
        )

    monkeypatch.setattr(
        "urllib3.connectionpool.HTTPConnectionPool.urlopen", urlopen_error
    )


@pytest.fixture
def shared_request_mocker():
    """
    Shared requests_mock.Mocker for all request mocking in tests

    When mocking multiple requests for one test, all mocking must be done using one
    Mocker object.
    """
    with requests_mock.Mocker() as m:
        yield m


@pytest.fixture
def metax_api():
    return MetaxAPI()


def _get_file_as_string(filename):
    """Return given file as string."""
    with open(filename) as infile:
        return infile.read()


@pytest.fixture
def metax_base_url():
    """Metax API"""
    return "https://metax-service.fd-staging.csc.fi/v3"


@pytest.fixture
def kielipankki_datacatalog_id():
    """Data catalog identifier for Kielipankki data."""
    return "urn:nbn:fi:att:data-catalog-kielipankki"


@pytest.fixture
def mock_get_response_json():
    """A mocked response for a single dataset from Metax"""
    file_string = _get_file_as_string(
        "tests/test_data/metax_single_record_response.json"
    )
    return json.loads(file_string)


@pytest.fixture
def dataset_pid():
    """Return PID of sample record."""
    return "urn.fi/urn:nbn:fi:lb-2016101210"


@pytest.fixture
def mock_requests_get_record(
    shared_request_mocker,
    mock_get_response_json,
    metax_base_url,
    kielipankki_datacatalog_id,
    dataset_pid,
):
    """A mock GET request to Metax."""
    shared_request_mocker.get(
        f"{metax_base_url}/datasets?data_catalog__id={kielipankki_datacatalog_id}&"
        f"persistent_identifier={dataset_pid}",
        json=mock_get_response_json,
    )
    return shared_request_mocker


@pytest.fixture
def mock_post_put_response_json():
    """Mock a response from Metax when making PUT or POST request."""
    file_string = _get_file_as_string("tests/test_data/put_post_response.json")
    return json.loads(file_string)


@pytest.fixture
def mock_requests_post(
    shared_request_mocker, mock_post_put_response_json, metax_base_url
):
    """Mock a post request to Metax."""
    shared_request_mocker.post(
        f"{metax_base_url}/datasets", json=mock_post_put_response_json
    )
    return shared_request_mocker


@pytest.fixture
def metax_dataset_id():
    return "1f32f478-8e7e-4d72-9638-d29a4f1430aa"


@pytest.fixture
def mock_requests_put(
    shared_request_mocker, mock_post_put_response_json, metax_base_url, metax_dataset_id
):
    """Mock a PUT request of a dataset to Metax."""
    shared_request_mocker.put(
        f"{metax_base_url}/datasets/{metax_dataset_id}",
        json=mock_post_put_response_json,
    )
    return shared_request_mocker


@pytest.fixture
def mock_metashare_record_not_found_in_datacatalog(
    shared_request_mocker, metax_base_url
):
    """
    Mock Metashare dataset requests to always report that PID does not
    exist.
    """
    dataset_request_matcher = re.compile(f"{metax_base_url}/datasets")
    shared_request_mocker.get(
        dataset_request_matcher, json={"count": 0, "results": [], "next": None}
    )
    return shared_request_mocker


@pytest.fixture
def mock_metashare_record_found_in_datacatalog(
    shared_request_mocker, metax_base_url, metax_dataset_id, dataset_pid
):
    """
    Mock Metashare dataset requests to always report that PID exists.
    """
    dataset_request_matcher = re.compile(f"{metax_base_url}/datasets")
    shared_request_mocker.get(
        dataset_request_matcher,
        json={
            "count": 1,
            "results": [{"id": metax_dataset_id, "persistent_identifier": dataset_pid}],
            "next": None,
        },
    )
    return shared_request_mocker


@pytest.fixture
def mock_delete_record(shared_request_mocker, metax_dataset_id, metax_base_url):
    """
    Mock a DELETE request to Metax.
    """
    shared_request_mocker.delete(
        f"{metax_base_url}/datasets/{metax_dataset_id}",
        status_code=204,
    )
    return shared_request_mocker


@pytest.fixture
def mock_pids_list_in_datacatalog(
    shared_request_mocker, metax_base_url, kielipankki_datacatalog_id, dataset_pid
):
    """
    Mock a GET request to fetch all PIDs in a datacatalog.
    """
    pid_data = {
        "results": [
            {"persistent_identifier": dataset_pid},
            {"persistent_identifier": "pid2"},
        ],
        "next": None,
    }
    shared_request_mocker.get(
        f"{metax_base_url}/datasets?data_catalog__id={kielipankki_datacatalog_id}&limit=100",
        json=pid_data,
    )
    return [dataset_pid, "pid2"]


@pytest.fixture
def mock_pids_list_in_datacatalog_matching_metashare(
    shared_request_mocker, metax_base_url, kielipankki_datacatalog_id, dataset_pid
):
    """
    Mock a GET request to fetch all PIDs in a datacatalog that matches those in Metashare.

    This fixture needed for testing "syncing" operation.
    """
    pid_data = {
        "results": [{"persistent_identifier": dataset_pid}],
        "next": None,
    }
    shared_request_mocker.get(
        f"{metax_base_url}/datasets?data_catalog__id={kielipankki_datacatalog_id}&limit=100",
        json=pid_data,
    )
    return [dataset_pid]


@pytest.fixture
def metashare_single_record_xml():
    """
    Metashare ListRecords output that contains a single record.
    """
    return _get_file_as_string("tests/test_data/kielipankki_record_sample.xml")


@pytest.fixture
def metashare_multiple_records_xml():
    """
    Metashare ListRecords output that contains a multiple records.
    """
    return _get_file_as_string(
        "tests/test_data/kielipankki_record_sample_multiple_records.xml"
    )


@pytest.fixture
def kielipankki_api_url():
    """
    The URL of the OAI-PMH API used in tests.
    """
    return "https://kielipankki.fi/md_api/que?metadataPrefix=info&verb=ListRecords"


@pytest.fixture
def mock_single_pid_list_from_metashare(
    shared_request_mocker, kielipankki_api_url, metashare_single_record_xml, dataset_pid
):
    """
    Mock a list of PIDs fetched from Metashare records.
    """
    shared_request_mocker.get(kielipankki_api_url, text=metashare_single_record_xml)
    return [dataset_pid]


@pytest.fixture
def mock_corpus_pid_list_from_metashare(
    shared_request_mocker, kielipankki_api_url, metashare_multiple_records_xml
):
    """
    Mock the record list to contain numerous resources, four of which are corpora.

    :return: PIDs for the corpora
    """
    shared_request_mocker.get(kielipankki_api_url, text=metashare_multiple_records_xml)
    return [
        "urn.fi/urn:nbn:fi:lb-2017021609",
        "urn.fi/urn:nbn:fi:lb-20140730196",
        "urn.fi/urn:nbn:fi:lb-2018060403",
        "urn.fi/urn:nbn:fi:lb-2019121004",
    ]

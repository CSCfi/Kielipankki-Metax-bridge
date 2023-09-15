import pytest
import requests_mock

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
    return "urn:nbn:fi:att:data-catalog-kielipankki-v4"

@pytest.fixture
def mock_get_response_json():
    """A mocked response for a single dataset from Metax"""
    return _get_file_as_string("tests/test_data/metax_single_record_response.json")

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
        text=mock_get_response_json,
    )
    return shared_request_mocker


@pytest.fixture
def mock_post_put_response_json():
    """Mock a response from Metax when making PUT or POST request."""
    return _get_file_as_string("tests/test_data/put_post_response.json")

@pytest.fixture
def mock_requests_post(
    shared_request_mocker, mock_post_put_response_json, metax_base_url
):
    """Mock a post request to Metax."""
    shared_request_mocker.post(
        f"{metax_base_url}/datasets", text=mock_post_put_response_json
    )
    return shared_request_mocker


@pytest.fixture
def metax_dataset_id():
    return "441560f5-4c2a-48eb-bc1a-489639ec3573"


@pytest.fixture
def mock_requests_put(
    shared_request_mocker, mock_post_put_response_json, metax_base_url, metax_dataset_id
):
    """Mock a PUT request of a dataset to Metax."""
    shared_request_mocker.put(
        f"{metax_base_url}/datasets/{metax_dataset_id}",
        text=mock_post_put_response_json,
    )
    return shared_request_mocker

import re
import json

from lxml import etree
import pytest
import requests_mock

from harvester.metadata_parser import MSRecordParser
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
def default_metax_api_log_file_path(tmp_path):
    """
    Temporary log file for logging Metax API calls
    """
    return tmp_path / "metax.log"


@pytest.fixture
def metax_api(
    default_metax_api_log_file_path, kielipankki_datacatalog_id, metax_base_url
):
    return MetaxAPI(
        base_url=metax_base_url,
        catalog_id=kielipankki_datacatalog_id,
        api_token="dummyapitoken",
        api_request_log_path=str(default_metax_api_log_file_path),
    )


def _get_file_as_string(filename):
    """Return given file as string."""
    with open(filename) as infile:
        return infile.read()


@pytest.fixture
def metax_base_url():
    """Metax API"""
    return "https://metax.fd-rework.csc.fi/v3"


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
def mock_pids_in_datacatalog(
    shared_request_mocker, metax_base_url, kielipankki_datacatalog_id, dataset_pid
):
    """
    Mock a GET request to fetch all PIDs in a datacatalog.

    :return: A set containing the PIDs corresponding to the mocked request
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
    return {dataset_pid, "pid2"}


@pytest.fixture
def mock_pids_in_datacatalog_matching_metashare(
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
    return {dataset_pid}


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
def metashare_no_records_xml():
    """
    Metashare ListRecords output that contains no records
    """
    return _get_file_as_string("tests/test_data/kielipankki_no_records.xml")


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


@pytest.fixture
def mock_metashare_get_single_record(
    shared_request_mocker, kielipankki_api_url, metashare_single_record_xml
):
    """
    Mock a GET request to the Metashare API to return XML with a single record.

    :return: The corresponding metadata as a list of dicts, one dict per record
    """
    shared_request_mocker.get(kielipankki_api_url, text=metashare_single_record_xml)
    yield [
        {
            "data_catalog": "urn:nbn:fi:att:data-catalog-kielipankki",
            "language": [{"url": "http://lexvo.org/id/iso639-3/fin"}],
            "field_of_science": [
                {"url": "http://www.yso.fi/onto/okm-tieteenala/ta6121"}
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
            "modified": "2017-02-15T00:00:00Z",
            "created": "2017-02-15T00:00:00Z",
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


@pytest.fixture
def mock_metashare_get_multiple_records(
    shared_request_mocker, kielipankki_api_url, metashare_multiple_records_xml
):
    """
    Mock a GET request to the Metashare API to return XML with a multiple records record.
    """
    shared_request_mocker.get(kielipankki_api_url, text=metashare_multiple_records_xml)


@pytest.fixture
def mock_metashare_get_no_new_records(
    shared_request_mocker,
    kielipankki_api_url,
    metashare_no_records_xml,
    latest_harvest_timestamp,
):
    """
    Mock a GET request to the Metashare API to return XML with no new records since
    latest harvest.
    """
    shared_request_mocker.get(
        kielipankki_api_url,
        text=metashare_no_records_xml,
    )


@pytest.fixture
def latest_harvest_timestamp():
    """
    A standardized last harvest date timestamp.
    """
    return "2023-09-08T14:34:16Z"


@pytest.fixture
def basic_metashare_record():
    """Well-formed record sample of Kielipankki metadata."""
    with open("tests/test_data/kielipankki_record_sample.xml") as xmlfile:
        return MSRecordParser(etree.fromstring(xmlfile.read()))


@pytest.fixture
def license_with_custom_url_record():
    """A record containing lisence url in documentation elements."""
    with open("tests/test_data/res_with_license_url.xml") as xmlfile:
        return MSRecordParser(etree.fromstring(xmlfile.read()))


@pytest.fixture
def language_vocabulary_endpoint_url():
    """
    URL for "fetching" the language vocabulary in tests.
    """
    return "https://metax.fairdata.fi/es/reference_data/language/_search?size=10000"


@pytest.fixture(autouse=True)
def mock_metax_language_vocabulary_endpoint(
    language_vocabulary_endpoint_url, shared_request_mocker
):
    """
    Make GET requests to the "Metax language vocabulary endpoint" return a small mocked
    response.

    This response will only list five languages/language families:
     - http://lexvo.org/id/iso639-3/kaf
     - http://lexvo.org/id/iso639-5/smi
     - http://lexvo.org/id/iso639-3/eng
     - http://lexvo.org/id/iso639-3/fin
     - http://lexvo.org/id/iso639-3/swe

    The response dicts are also somewhat trimmed down, because we don't need the full
    data with all translations.
    """
    with open("tests/test_data/metax_language_vocabulary.json", "r") as response_json:
        data = json.loads(response_json.read())
    shared_request_mocker.get(language_vocabulary_endpoint_url, json=data)

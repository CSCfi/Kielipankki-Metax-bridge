import re
import json
import yaml

from click.testing import CliRunner
from lxml import etree
import pytest
import requests_mock

from harvester import language_validator
from harvester.metadata_parser import RecordParser
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
    return "https://metax.demo.fairdata.fi/v3"


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
    return "urn:nbn:fi:lb-2017021609"


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
def mock_cmdi_record_not_found_in_datacatalog(shared_request_mocker, metax_base_url):
    """
    Mock Metax dataset requests to always report that PID does not
    exist.
    """
    dataset_request_matcher = re.compile(f"{metax_base_url}/datasets")
    shared_request_mocker.get(
        dataset_request_matcher, json={"count": 0, "results": [], "next": None}
    )
    return shared_request_mocker


@pytest.fixture
def mock_cmdi_record_found_in_datacatalog(
    shared_request_mocker, metax_base_url, metax_dataset_id, dataset_pid
):
    """
    Mock Metax dataset requests to always report that PID exists.
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
def mock_pids_in_datacatalog_matching_cmdi(
    shared_request_mocker, metax_base_url, kielipankki_datacatalog_id, dataset_pid
):
    """
    Mock a GET to fetch all PIDs in a datacatalog that matches those in CMDI records.

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
def cmdi_single_record_xml():
    """
    Single record element in CMDI format.
    """
    return _get_file_as_string("tests/test_data/kielipankki_record_sample.xml")


@pytest.fixture
def cmdi_multiple_records_xml():
    """
    Comedi ListRecords output that contains a multiple records.
    """
    return _get_file_as_string("tests/test_data/comedi_list_records_multiple.xml")


@pytest.fixture
def cmdi_no_records_xml():
    """
    Comedi ListRecords output that contains no records
    """
    return _get_file_as_string("tests/test_data/comedi_list_records_no_records.xml")


@pytest.fixture
def kielipankki_api_url():
    """
    The URL of the OAI-PMH API used in tests.
    """
    return "https://clarino.uib.no/oai?metadataPrefix=cmdi"


@pytest.fixture
def mock_single_pid_list_from_cmdi(
    shared_request_mocker, kielipankki_api_url, cmdi_single_record_xml, dataset_pid
):
    """
    Mock a list of PIDs fetched from Comedi containing a single PID.
    """
    shared_request_mocker.get(kielipankki_api_url, text=cmdi_single_record_xml)
    return [dataset_pid]


@pytest.fixture
def mock_corpus_pid_list_from_cmdi(
    shared_request_mocker, kielipankki_api_url, cmdi_multiple_records_xml
):
    """
    Mock the record list to contain numerous resources, four of which are corpora.

    :return: PIDs for the corpora
    """
    shared_request_mocker.get(kielipankki_api_url, text=cmdi_multiple_records_xml)
    return [
        "urn:nbn:fi:lb-2017021609",
        "urn:nbn:fi:lb-20140730196",
        "urn:nbn:fi:lb-2018060403",
        "urn:nbn:fi:lb-2019121004",
    ]


@pytest.fixture
def mock_list_records_no_in_progress(
    shared_request_mocker,
    kielipankki_api_url,
    cmdi_no_records_xml,
):
    """
    Mock a GET ListRecords for in progress records to return XML with no records.

    :return: The corresponding metadata as a list of dicts, one dict per record
    """
    shared_request_mocker.get(
        kielipankki_api_url + "&status=in-progress", text=cmdi_no_records_xml
    )


@pytest.fixture
def mock_list_records_single_record(shared_request_mocker, kielipankki_api_url):
    """
    Mock a GET ListRecords to return XML with a single record.

    :return: The corresponding metadata as a list of dicts, one dict per record
    """
    response_text = _get_file_as_string(
        "tests/test_data/comedi_list_records_single.xml"
    )
    shared_request_mocker.get(
        kielipankki_api_url + "&status=published", text=response_text
    )
    yield [
        {
            "data_catalog": "urn:nbn:fi:att:data-catalog-kielipankki",
            "language": [{"url": "http://lexvo.org/id/iso639-3/fin"}],
            "field_of_science": [
                {"url": "http://www.yso.fi/onto/okm-tieteenala/ta6121"}
            ],
            "persistent_identifier": "urn:nbn:fi:lb-2017021609",
            "title": {
                "en": "Silva Kiuru's Time Expressions Corpus",
                "fi": "Silva Kiurun ajanilmausaineisto",
            },
            "description": {
                "en": "This corpus of time expressions has been compiled from literary works, translations, dialect texts as well as other texts. Format: word documents.",
                "fi": "Tämä suomen kielen ajanilmauksia käsittävä aineisto on koottu kaunokirjallisten alkuperäisteosten, käännösten, murreaineistojen ja muiden tekstien pohjalta.",
            },
            "modified": "2024-06-19T07:38:46Z",
            "created": "2022-09-02T00:00:00Z",
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
            "actors": [
                {
                    "organization": {
                        "url": "http://uri.suomi.fi/codelist/fairdata/organization/code/01901",
                    },
                    "person": {
                        "email": "diana@example.com",
                        "name": "Diana Datankerääjä",
                    },
                    "roles": ["creator"],
                },
                {
                    "organization": {
                        "url": "http://uri.suomi.fi/codelist/fairdata/organization/code/01901",
                    },
                    "roles": [
                        "publisher",
                        "rights_holder",
                    ],
                },
                {
                    "roles": ["curator"],
                    "person": {
                        "name": "Kiia Kontakti",
                        "email": "kiia@example.com",
                    },
                    "organization": {
                        "url": "http://uri.suomi.fi/codelist/fairdata/organization/code/01901",
                    },
                },
            ],
            "state": "published",
        }
    ]


@pytest.fixture
def mock_list_records_single_record_in_progress(
    shared_request_mocker, kielipankki_api_url
):
    """
    Mock a GET ListRecords for in progress records to return XML with a single record.

    :return: The corresponding metadata as a list of dicts, one dict per record
    """
    response_text = _get_file_as_string(
        "tests/test_data/comedi_list_records_single.xml"
    )
    shared_request_mocker.get(
        kielipankki_api_url + "&status=in-progress", text=response_text
    )
    yield [
        {
            "data_catalog": "urn:nbn:fi:att:data-catalog-kielipankki",
            "language": [{"url": "http://lexvo.org/id/iso639-3/fin"}],
            "field_of_science": [
                {"url": "http://www.yso.fi/onto/okm-tieteenala/ta6121"}
            ],
            "persistent_identifier": "urn:nbn:fi:lb-2017021609",
            "title": {
                "en": "Silva Kiuru's Time Expressions Corpus",
                "fi": "Silva Kiurun ajanilmausaineisto",
            },
            "description": {
                "en": "This corpus of time expressions has been compiled from literary works, translations, dialect texts as well as other texts. Format: word documents.",
                "fi": "Tämä suomen kielen ajanilmauksia käsittävä aineisto on koottu kaunokirjallisten alkuperäisteosten, käännösten, murreaineistojen ja muiden tekstien pohjalta.",
            },
            "modified": "2024-06-19T07:38:46Z",
            "created": "2022-09-02T00:00:00Z",
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
            "actors": [
                {
                    "organization": {
                        "url": "http://uri.suomi.fi/codelist/fairdata/organization/code/01901",
                    },
                    "person": {
                        "email": "diana@example.com",
                        "name": "Diana Datankerääjä",
                    },
                    "roles": ["creator"],
                },
                {
                    "organization": {
                        "url": "http://uri.suomi.fi/codelist/fairdata/organization/code/01901",
                    },
                    "roles": [
                        "publisher",
                        "rights_holder",
                    ],
                },
                {
                    "roles": ["curator"],
                    "person": {
                        "name": "Kiia Kontakti",
                        "email": "kiia@example.com",
                    },
                    "organization": {
                        "url": "http://uri.suomi.fi/codelist/fairdata/organization/code/01901",
                    },
                },
            ],
            # NB: this is hard-coded in the metadata parser: it cannot determine it from
            # the record, nor are we expecting to send unpublished records to Metax.
            "state": "published",
        }
    ]


@pytest.fixture
def mock_cmdi_get_single_record(
    shared_request_mocker, kielipankki_api_url, cmdi_single_record_xml
):
    """
    Mock a GET request to Comedi to return XML with a single record.

    :return: The corresponding metadata as a list of dicts, one dict per record
    """
    shared_request_mocker.get(kielipankki_api_url, text=cmdi_single_record_xml)
    yield [
        {
            "data_catalog": "urn:nbn:fi:att:data-catalog-kielipankki",
            "language": [{"url": "http://lexvo.org/id/iso639-3/fin"}],
            "field_of_science": [
                {"url": "http://www.yso.fi/onto/okm-tieteenala/ta6121"}
            ],
            "persistent_identifier": "urn:nbn:fi:lb-2017021609",
            "title": {
                "en": "Silva Kiuru's Time Expressions Corpus",
                "fi": "Silva Kiurun ajanilmausaineisto",
            },
            "description": {
                "en": "This corpus of time expressions has been compiled from literary works, translations, dialect texts as well as other texts. Format: word documents.",
                "fi": "Tämä suomen kielen ajanilmauksia käsittävä aineisto on koottu kaunokirjallisten alkuperäisteosten, käännösten, murreaineistojen ja muiden tekstien pohjalta.",
            },
            "modified": "2024-06-19T07:38:46Z",
            "created": "2022-09-02T00:00:00Z",
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
            "actors": [
                {
                    "organization": None,
                    "person": {
                        "email": "imre.bartis@helsinki.fi",
                        "name": "Imre Bartis",
                    },
                    "roles": ["creator"],
                },
                {
                    "organization": {
                        "url": "http://uri.suomi.fi/codelist/fairdata/organization/code/01901",
                    },
                    "roles": ["curator"],
                    "person": {
                        "name": "Mari Siiroinen",
                        "email": "mari.siiroinen@helsinki.fi",
                    },
                },
            ],
            "state": "published",
        }
    ]


@pytest.fixture
def mock_cmdi_get_multiple_records(
    shared_request_mocker, kielipankki_api_url, cmdi_multiple_records_xml
):
    """
    Mock a GET request to Comedi to return XML with a multiple records record.
    """
    shared_request_mocker.get(kielipankki_api_url, text=cmdi_multiple_records_xml)


@pytest.fixture
def mock_cmdi_get_no_new_records(
    shared_request_mocker,
    kielipankki_api_url,
    cmdi_no_records_xml,
    latest_harvest_timestamp,
):
    """
    Mock a GET request to Comedi to return XML with no new records since
    latest harvest.
    """
    shared_request_mocker.get(
        kielipankki_api_url,
        text=cmdi_no_records_xml,
    )


@pytest.fixture
def latest_harvest_timestamp():
    """
    A standardized last harvest date timestamp.
    """
    return "2023-09-08T14:34:16Z"


@pytest.fixture
def basic_cmdi_record():
    """Well-formed record sample of Kielipankki metadata."""
    with open("tests/test_data/kielipankki_record_sample.xml") as xmlfile:
        return RecordParser(etree.fromstring(xmlfile.read()))


@pytest.fixture
def license_with_custom_url_record():
    """A record containing lisence url in documentation elements."""
    with open("tests/test_data/res_with_license_url.xml") as xmlfile:
        return RecordParser(etree.fromstring(xmlfile.read()))


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
    language_validator._allowed_language_uris.cache_clear()
    with open("tests/test_data/metax_language_vocabulary.json", "r") as response_json:
        data = json.loads(response_json.read())
    shared_request_mocker.get(language_vocabulary_endpoint_url, json=data)


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
def default_test_log_file_path(tmp_path):
    return tmp_path / "harvester_test.log"


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
            "metax_base_url": "https://metax.demo.fairdata.fi/v3",
            "metax_catalog_id": "urn:nbn:fi:att:data-catalog-kielipankki",
            "harvester_log_file": str(default_test_log_file_path),
            "metax_api_log_file": str(default_metax_api_log_file_path),
            "save_records_locally": False,
        }
    )


@pytest.fixture
def run_cli(basic_configuration):
    """
    Helper for running the command line interface with given arguments.

    If some argument is not specified, default testing values are used.

    File arguments can be specified as strings or Paths: they are automatically
    converted.
    """

    def _run_cli(cli_function, configuration_file_path=None, extra_args=None):
        if configuration_file_path is None:
            configuration_file_path = basic_configuration

        required_args = [str(configuration_file_path)]
        if not extra_args:
            extra_args = []

        runner = CliRunner(mix_stderr=False)
        return runner.invoke(cli_function, required_args + extra_args)

    return _run_cli

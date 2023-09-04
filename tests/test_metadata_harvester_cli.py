import pytest
import requests
import requests_mock
from lxml import etree
from click.testing import CliRunner
import metadata_harvester_cli
import os

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
def kielipankki_api_url():
    """
    The URL of the OAI-PMH API used in tests.
    """
    return "https://kielipankki.fi/md_api/que?metadataPrefix=info&verb=ListRecords"

def _get_file_as_string(filename):
    with open(filename) as infile:
        return infile.read()

@pytest.fixture
def single_record_xml():
    return _get_file_as_string("tests/test_data/kielipankki_record_sample.xml")

@pytest.fixture
def single_record_response(kielipankki_api_url):
    """
    A GET request that returns the XML data as a dictionary
    """
    record = _get_file_as_string("tests/test_data/kielipankki_record_sample.xml")
    
    with requests_mock.Mocker() as mocker:
        mocker.get(kielipankki_api_url, text=record)

        yield {"persistent_identifier": "urn.fi/urn:nbn:fi:lb-2017021609", 
               "title": {
                   "en": "Silva Kiuru's Time Expressions Corpus", 
                   "fi": "Silva Kiurun ajanilmausaineisto"}, 
               "description": {
                   "en": "This corpus of time expressions has been compiled from literary works, translations, dialect texts as well as other texts. Format: word documents.", 
                   "fi": "T\\u00e4m\\u00e4 suomen kielen ajanilmauksia k\\u00e4sitt\\u00e4v\\u00e4 aineisto on koottu kaunokirjallisten alkuper\\u00e4isteosten, k\\u00e4\\u00e4nn\\u00f6sten, murreaineistojen ja muiden tekstien pohjalta."}, 
               "modified": "2017-02-15T00:00:00.000000Z", 
               "issued": "2017-02-15T00:00:00.000000Z",
               "access_rights": {
        "license": [
            {
                "url": "http://uri.suomi.fi/codelist/fairdata/license/code/undernegotiation"
            }
        ],
        "access_type": {
            "access_type": "http://uri.suomi.fi/codelist/fairdata/access_type/code/open"
        }
    }
               }

#These tests hang when get_all_metadata_records() does not have a limit. When changing value to "1", the tests pass without problems.
def test_defined_url(single_record_xml, single_record_response, kielipankki_api_url):
    """
    Test that the CLI can fetch records from a specific URL
    """
    runner = CliRunner()

    result = runner.invoke(retrieve_metadata_content, ["--url", kielipankki_api_url], input=single_record_xml)
    print(result)

    assert single_record_response["persistent_identifier"] in result.output
    assert single_record_response["title"]["en"] in result.output
    assert single_record_response["title"]["fi"] in result.output
    assert single_record_response["modified"] in result.output
    assert single_record_response["issued"] in result.output
    assert single_record_response["description"]["en"] in result.output
    assert single_record_response["description"]["fi"] in result.output 

def test_default_url(single_record_xml, single_record_response):
    """
    Test that the CLI can fetch records from the default URL
    """
    runner = CliRunner()

    result = runner.invoke(retrieve_metadata_content, input=single_record_xml)

    assert single_record_response["persistent_identifier"] in result.output
    assert single_record_response["title"]["en"] in result.output
    assert single_record_response["title"]["fi"] in result.output
    assert single_record_response["modified"] in result.output
    assert single_record_response["issued"] in result.output
    assert single_record_response["description"]["en"] in result.output
    assert single_record_response["description"]["fi"] in result.output

@pytest.fixture
def create_test_log_file():
    """Create a temporary log file for testing and clean up afterwards."""
    log_file = "harvester.log"
    log_file_data = [
        "2023-08-30 09:00:00 - INFO - Success\n",
        "2023-08-31 10:00:00 - INFO - Success\n",
        "2023-09-01 11:00:00 - INFO - Success\n",
    ]
    with open(log_file, "w") as file:
        file.writelines(log_file_data)
    yield
    os.remove(log_file)

def test_get_last_harvest_date(create_test_log_file):
    last_harvest_date =metadata_harvester_cli.get_last_harvest_date()
    assert last_harvest_date == "2023-09-01"

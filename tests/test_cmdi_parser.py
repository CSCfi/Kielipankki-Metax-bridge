import pytest
from lxml import etree
import json
from harvester.cmdi_parser import MSRecordParser


def _get_file_as_lxml(filename):
    with open(filename) as infile:
        return etree.fromstring(infile.read())

@pytest.fixture
def single_record():
    return _get_file_as_lxml("tests/test_data/kielipankki_record_sample.xml")

def test_get_title(single_record):
    record = MSRecordParser(single_record)
    result = record._get_language_contents("//info:resourceName")
    expected_result = {"en": "Silva Kiuru's Time Expressions Corpus", "fi": "Silva Kiurun ajanilmausaineisto"}
    assert result == expected_result

def test_get_description(single_record):
    record = MSRecordParser(single_record)
    result = record._get_language_contents("//info:description")
    expected_result = {"en": "This corpus of time expressions has been compiled from literary works, translations, dialect texts as well as other texts. Format: word documents.", "fi": "T\u00e4m\u00e4 suomen kielen ajanilmauksia k\u00e4sitt\u00e4v\u00e4 aineisto on koottu kaunokirjallisten alkuper\u00e4isteosten, k\u00e4\u00e4nn\u00f6sten, murreaineistojen ja muiden tekstien pohjalta."}
    assert result == expected_result

def test_get_identifier(single_record):
    record = MSRecordParser(single_record)
    result = record._get_identifier("//info:identificationInfo/info:identifier/text()")
    expected_result = "urn.fi/urn:nbn:fi:lb-2017021609"
    assert result == expected_result

def test_get_modified_date(single_record):
    record = MSRecordParser(single_record)
    result = record._get_date("//info:metadataInfo/info:metadataLastDateUpdated/text()")
    expected_result = "2017-02-15T00:00:00.000000Z"
    assert result == expected_result

def test_get_issued_date(single_record):
    record = MSRecordParser(single_record)
    result = record._get_date("//info:metadataInfo/info:metadataCreationDate/text()")
    expected_result = "2017-02-15T00:00:00.000000Z"
    assert result == expected_result
 
def test_get_title_not_empty(single_record):
    record = MSRecordParser(single_record)
    result = record._get_language_contents("//info:resourceName")
    assert result != {}

def test_get_description_not_empty(single_record):
    record = MSRecordParser(single_record)
    result = record._get_language_contents("//info:description")
    assert result != {}

def test_get_identifier_not_empty(single_record):
    record = MSRecordParser(single_record)
    result = record._get_identifier("//info:identificationInfo/info:identifier/text()")
    assert result != ""

def test_get_modified_not_empty(single_record):
    record = MSRecordParser(single_record)
    result = record._get_date("//info:metadataInfo/info:metadataLastDateUpdated/text()")
    assert result != ""

def test_get_issued_not_empty(single_record):
    record = MSRecordParser(single_record)
    result = record._get_date("//info:metadataInfo/info:metadataCreationDate/text()")
    assert result != ""

def test_json_converter(single_record):
    record = MSRecordParser(single_record)
    result = record.json_converter()
    expected_result = json.dumps(
        {
    "data_catalog": "urn:nbn:fi:att:data-catalog-kielipankki",
    "language": [
        {
            "url": "http://lexvo.org/id/iso639-3/fin"
        }
    ],
    "field_of_science": [
        {
            "url": "http://www.yso.fi/onto/okm-tieteenala/ta112"
        }
    ],
    "persistent_identifier": "urn.fi/urn:nbn:fi:lb-2017021609",
    "title": {
        "en": "Silva Kiuru's Time Expressions Corpus",
        "fi": "Silva Kiurun ajanilmausaineisto"
    },
    "description": {
        "en": "This corpus of time expressions has been compiled from literary works, translations, dialect texts as well as other texts. Format: word documents.",
        "fi": "T\u00e4m\u00e4 suomen kielen ajanilmauksia k\u00e4sitt\u00e4v\u00e4 aineisto on koottu kaunokirjallisten alkuper\u00e4isteosten, k\u00e4\u00e4nn\u00f6sten, murreaineistojen ja muiden tekstien pohjalta."
    },
    "modified": "2017-02-15T00:00:00.000000Z",
    "issued": "2017-02-15T00:00:00.000000Z",
    "access_rights": {
        "license": [
            {
                "url": "http://uri.suomi.fi/codelist/fairdata/license/code/undernegotiation"
            }
        ],
        "access_type": {
            "url": "http://uri.suomi.fi/codelist/fairdata/access_type/code/open"
        }
    }
}
    )
    assert result == expected_result

@pytest.fixture
def deleted_record():
    return _get_file_as_lxml("tests/test_data/deleted_record.xml")

def test_deleted_record(deleted_record):
    deleted_record = MSRecordParser(deleted_record)
    result = deleted_record.json_converter()
    expected_result = None
    assert result == expected_result

@pytest.fixture
def tool_record():
    return _get_file_as_lxml("tests/test_data/tool_record.xml")

def test_tool_record(tool_record):
    tool_record = MSRecordParser(tool_record)
    result = tool_record.json_converter()
    expected_result = None
    assert result == expected_result

@pytest.fixture
def missing_license_record():
    return _get_file_as_lxml("tests/test_data/missing_licenseinfo.xml")

def test_missing_license_record(missing_license_record):
    missing_license_record = MSRecordParser(missing_license_record)
    result = missing_license_record._map_access_rights()
    expected_result = {
                "license": [
                    {
                        "url": "http://uri.suomi.fi/codelist/fairdata/license/code/other"
                    }
                ],
                "access_type": {
                    "url": "http://uri.suomi.fi/codelist/fairdata/access_type/code/restricted"
                }
        }
    assert result == expected_result

@pytest.fixture
def license_with_custom_url_record():
    return _get_file_as_lxml("tests/test_data/res_with_license_url.xml")

def test_license_custom_url_record(license_with_custom_url_record):
    license_with_custom_url_record = MSRecordParser(license_with_custom_url_record)
    result = license_with_custom_url_record._map_access_rights()
    expected_result = {
        "license": [
            {
                "url": "http://uri.suomi.fi/codelist/fairdata/license/code/ClarinRES-1.0",
                "custom_url": "https://www.kielipankki.fi/lic/dma-wn/?lang=en"
            }
        ],
        "access_type": {
            "url": "http://uri.suomi.fi/codelist/fairdata/access_type/code/restricted"
        }
        }
    assert result == expected_result

@pytest.fixture
def several_licenses_record():
    return _get_file_as_lxml("tests/test_data/several_licenses.xml")

def test_several_licenses_record(several_licenses_record):
    several_licenses_record = MSRecordParser(several_licenses_record)
    result = several_licenses_record._map_access_rights()
    expected_result = {
        "access_type": {
            "url": "http://uri.suomi.fi/codelist/fairdata/access_type/code/restricted"
        },
        "license": [
            {
                "url": "http://uri.suomi.fi/codelist/fairdata/license/code/ClarinRES-1.0",
                "custom_url": "http://urn.fi/urn:nbn:fi:lb-2022020223"
            },
            {
                "url": "http://uri.suomi.fi/codelist/fairdata/license/code/other",
                "custom_url": "http://urn.fi/urn:nbn:fi:lb-2022020223"
            }
        ]
            }
    assert result == expected_result

@pytest.fixture
def accesstype_open_record():
    return _get_file_as_lxml("tests/test_data/accesstype_open.xml")

def test_accesstype_open_record(accesstype_open_record):
    accesstype_open_record = MSRecordParser(accesstype_open_record)
    result = accesstype_open_record._map_access_rights()
    expected_result = {
        "license": [
            {
                "url": "http://uri.suomi.fi/codelist/fairdata/license/code/CC-BY-1.0",
                "custom_url": "http://urn.fi/urn:nbn:fi:lb-2022090204"
            }
        ],
        "access_type": {
            "url": "http://uri.suomi.fi/codelist/fairdata/access_type/code/open"
        }
            }
    assert result == expected_result

@pytest.fixture
def missing_pid_record():
    return _get_file_as_lxml("tests/test_data/missing_pid.xml")

def test_missing_pid_record(missing_pid_record):
    missing_pid_record = MSRecordParser(missing_pid_record)
    result = missing_pid_record.json_converter()
    expected_result = None
    assert result == expected_result

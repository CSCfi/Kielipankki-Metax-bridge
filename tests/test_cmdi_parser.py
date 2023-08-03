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
    result = record._get_language_contents("//cmd:resourceName")
    expected_result = {"en": "Silva Kiuru's Time Expressions Corpus", "fi": "Silva Kiurun ajanilmausaineisto"}
    assert result == expected_result

def test_get_description(single_record):
    record = MSRecordParser(single_record)
    result = record._get_language_contents("//cmd:description")
    expected_result = {"en": "This corpus of time expressions has been compiled from literary works, translations, dialect texts as well as other texts. Format: word documents.", "fi": "T\u00e4m\u00e4 suomen kielen ajanilmauksia k\u00e4sitt\u00e4v\u00e4 aineisto on koottu kaunokirjallisten alkuper\u00e4isteosten, k\u00e4\u00e4nn\u00f6sten, murreaineistojen ja muiden tekstien pohjalta."}
    assert result == expected_result

def test_get_identifier(single_record):
    record = MSRecordParser(single_record)
    result = record._get_identifier("//cmd:identificationInfo/cmd:identifier/text()")
    expected_result = "urn.fi/urn:nbn:fi:lb-2017021609"
    assert result == expected_result

def test_get_modified(single_record):
    record = MSRecordParser(single_record)
    result = record._get_text_xpath("//cmd:metadataInfo/cmd:metadataLastDateUpdated/text()")
    expected_result = "2017-02-15"
    assert result == expected_result

def test_get_issued(single_record):
    record = MSRecordParser(single_record)
    result = record._get_text_xpath("//cmd:metadataInfo/cmd:metadataCreationDate/text()")
    expected_result = "2017-02-15"
    assert result == expected_result
 
def test_get_title_not_empty(single_record):
    record = MSRecordParser(single_record)
    result = record._get_language_contents("//cmd:resourceName")
    assert result != {}

def test_get_description_not_empty(single_record):
    record = MSRecordParser(single_record)
    result = record._get_language_contents("//cmd:description")
    assert result != {}

def test_get_identifier_not_empty(single_record):
    record = MSRecordParser(single_record)
    result = record._get_identifier("//cmd:identificationInfo/cmd:identifier/text()")
    assert result != ""

def test_get_modified_not_empty(single_record):
    record = MSRecordParser(single_record)
    result = record._get_text_xpath("//cmd:metadataInfo/cmd:metadataLastDateUpdated/text()")
    assert result != ""

def test_get_issued_not_empty(single_record):
    record = MSRecordParser(single_record)
    result = record._get_text_xpath("//cmd:metadataInfo/cmd:metadataCreationDate/text()")
    assert result != ""

def test_json_converter(single_record):
    record = MSRecordParser(single_record)
    result = record.json_converter()
    expected_result = json.dumps({"persistent_identifier": "urn.fi/urn:nbn:fi:lb-2017021609", "title": {"en": "Silva Kiuru's Time Expressions Corpus", "fi": "Silva Kiurun ajanilmausaineisto"}, "description": {"en": "This corpus of time expressions has been compiled from literary works, translations, dialect texts as well as other texts. Format: word documents.", "fi": "T\u00e4m\u00e4 suomen kielen ajanilmauksia k\u00e4sitt\u00e4v\u00e4 aineisto on koottu kaunokirjallisten alkuper\u00e4isteosten, k\u00e4\u00e4nn\u00f6sten, murreaineistojen ja muiden tekstien pohjalta."}, "modified": "2017-02-15", "issued": "2017-02-15"})
    assert result == expected_result
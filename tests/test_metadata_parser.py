import pytest
from lxml import etree
from harvester.metadata_parser import MSRecordParser


def _get_file_as_lxml(filename):
    """Returns given file as an lxml element."""
    with open(filename) as infile:
        return etree.fromstring(infile.read())


def test_get_title(basic_metashare_record):
    """Testing that different language versions of "title" are mapped."""
    result = basic_metashare_record._get_element_text_in_preferred_language(
        "//info:resourceName"
    )
    expected_result = {
        "en": "Silva Kiuru's Time Expressions Corpus",
        "fi": "Silva Kiurun ajanilmausaineisto",
    }
    assert result == expected_result


def test_get_description(basic_metashare_record):
    """Testing that different language versions of "description" are mapped."""
    result = basic_metashare_record._get_element_text_in_preferred_language(
        "//info:description"
    )
    expected_result = {
        "en": "This corpus of time expressions has been compiled from literary works, translations, dialect texts as well as other texts. Format: word documents.",
        "fi": "T\u00e4m\u00e4 suomen kielen ajanilmauksia k\u00e4sitt\u00e4v\u00e4 aineisto on koottu kaunokirjallisten alkuper\u00e4isteosten, k\u00e4\u00e4nn\u00f6sten, murreaineistojen ja muiden tekstien pohjalta.",
    }
    assert result == expected_result


def test_pid(basic_metashare_record, dataset_pid):
    """Check that the correct PID is returned."""
    assert basic_metashare_record.pid == dataset_pid


def test_get_modified_datetime(basic_metashare_record):
    """Check that the modified date is returned in correct format."""
    result = basic_metashare_record._get_datetime(
        "//info:metadataInfo/info:metadataLastDateUpdated/text()"
    )
    expected_result = "2017-02-15T00:00:00Z"
    assert result == expected_result


def test_get_created_datetime(basic_metashare_record):
    """Check that the created date is returned in correct format."""
    result = basic_metashare_record._get_datetime(
        "//info:metadataInfo/info:metadataCreationDate/text()"
    )
    expected_result = "2017-02-15T00:00:00Z"
    assert result == expected_result


def test_to_dict(basic_metashare_record):
    """Test that all relevant elements are mapped to a dictionary."""
    result = basic_metashare_record.to_dict()
    expected_result = {
        "data_catalog": "urn:nbn:fi:att:data-catalog-kielipankki",
        "language": [{"url": "http://lexvo.org/id/iso639-3/fin"}],
        "field_of_science": [{"url": "http://www.yso.fi/onto/okm-tieteenala/ta112"}],
        "persistent_identifier": "urn.fi/urn:nbn:fi:lb-2016101210",
        "title": {
            "en": "Silva Kiuru's Time Expressions Corpus",
            "fi": "Silva Kiurun ajanilmausaineisto",
        },
        "description": {
            "en": "This corpus of time expressions has been compiled from literary works, translations, dialect texts as well as other texts. Format: word documents.",
            "fi": "T\u00e4m\u00e4 suomen kielen ajanilmauksia k\u00e4sitt\u00e4v\u00e4 aineisto on koottu kaunokirjallisten alkuper\u00e4isteosten, k\u00e4\u00e4nn\u00f6sten, murreaineistojen ja muiden tekstien pohjalta.",
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
    assert result == expected_result


@pytest.fixture
def tool_record():
    """A record where resourceType is "toolService", not "corpus"."""
    return MSRecordParser(_get_file_as_lxml("tests/test_data/tool_record.xml"))


def test_check_resourcetype_corpus(tool_record):
    """Check that a "toolService" record is spotted."""
    result = tool_record.check_resourcetype_corpus()
    assert result is None


@pytest.fixture
def missing_license_record():
    """A record missing licenseInfo element."""
    return MSRecordParser(_get_file_as_lxml("tests/test_data/missing_licenseinfo.xml"))


def test_missing_license_record(missing_license_record):
    """Test that records missing licenseInfo element is mapped as expected."""
    result = missing_license_record._map_access_rights()
    expected_result = {
        "license": [
            {"url": "http://uri.suomi.fi/codelist/fairdata/license/code/other"}
        ],
        "access_type": {
            "url": "http://uri.suomi.fi/codelist/fairdata/access_type/code/restricted"
        },
    }
    assert result == expected_result


def test_license_custom_url_record(license_with_custom_url_record):
    """Test that license details and availability are mapped."""
    result = license_with_custom_url_record._map_access_rights()
    expected_result = {
        "license": [
            {
                "url": "http://uri.suomi.fi/codelist/fairdata/license/code/ClarinRES-1.0",
                "custom_url": "https://www.kielipankki.fi/lic/dma-wn/?lang=en",
            }
        ],
        "access_type": {
            "url": "http://uri.suomi.fi/codelist/fairdata/access_type/code/restricted"
        },
    }
    assert result == expected_result


@pytest.fixture
def several_licenses_record():
    """A record that has several licenseInfo elements."""
    return MSRecordParser(_get_file_as_lxml("tests/test_data/several_licenses.xml"))


def test_several_licenses_record(several_licenses_record):
    """Check that all licenses of a record are parsed and mapped."""
    result = several_licenses_record._map_access_rights()
    expected_result = {
        "access_type": {
            "url": "http://uri.suomi.fi/codelist/fairdata/access_type/code/restricted"
        },
        "license": [
            {
                "url": "http://uri.suomi.fi/codelist/fairdata/license/code/ClarinRES-1.0",
                "custom_url": "http://urn.fi/urn:nbn:fi:lb-2022020223",
            },
            {
                "url": "http://uri.suomi.fi/codelist/fairdata/license/code/other",
                "custom_url": "http://urn.fi/urn:nbn:fi:lb-2022020223",
            },
        ],
    }
    assert result == expected_result


@pytest.fixture
def accesstype_open_record():
    """A record with a PUB license."""
    return MSRecordParser(_get_file_as_lxml("tests/test_data/accesstype_open.xml"))


def test_accesstype_open_record(accesstype_open_record):
    """Check that records with open access are parsed and mapped."""
    result = accesstype_open_record._map_access_rights()
    expected_result = {
        "license": [
            {
                "url": "http://uri.suomi.fi/codelist/fairdata/license/code/CC-BY-1.0",
                "custom_url": "http://urn.fi/urn:nbn:fi:lb-2022090204",
            }
        ],
        "access_type": {
            "url": "http://uri.suomi.fi/codelist/fairdata/access_type/code/open"
        },
    }
    assert result == expected_result


@pytest.fixture
def license_with_custom_url_in_doc_unstruct_record():
    """A record containing lisence url in documentation elements."""
    return MSRecordParser(
        _get_file_as_lxml("tests/test_data/license_in_doc_unstruct.xml")
    )


def test_custom_url_from_doc_unstruct_element(
    license_with_custom_url_in_doc_unstruct_record,
):
    """A record containing lisence url in documentation elements."""
    result = license_with_custom_url_in_doc_unstruct_record._map_access_rights()
    expected_result = {
        "license": [
            {
                "url": "http://uri.suomi.fi/codelist/fairdata/license/code/ClarinRES-1.0",
                "custom_url": "http://urn.fi/urn:nbn:fi:lb-2016112304",
            }
        ],
        "access_type": {
            "url": "http://uri.suomi.fi/codelist/fairdata/access_type/code/restricted"
        },
    }
    assert result == expected_result


@pytest.fixture
def missing_pid_record():
    """A record that doesn't have a PID."""
    return MSRecordParser(_get_file_as_lxml("tests/test_data/missing_pid.xml"))


def test_check_pid_exists(missing_pid_record):
    """Check that a missing PID is handled."""
    assert not missing_pid_record.check_pid_exists()


def test_get_resource_languages(basic_metashare_record):
    """
    Verify that a single language from a record is returned in Metax-approved format.

    Metax espects a list of dicts, each dict describing one language. The only
    information required for each language is a lexvo url.
    """
    assert basic_metashare_record._get_resource_languages() == [
        {"url": "http://lexvo.org/id/iso639-3/fin"}
    ]


def test_get_resource_languages_with_multiple_languages():
    """
    Check that multiple languages for one resource are reported properly. Also includes
    an ISO 639-5 language.
    """
    record = MSRecordParser(
        _get_file_as_lxml(
            "tests/test_data/kielipankki_record_sample_multiple_languages.xml"
        )
    )
    languages = record._get_resource_languages()
    assert len(languages) == 4

    language_urls = [language["url"] for language in languages]
    assert "http://lexvo.org/id/iso639-5/smi" in language_urls
    assert "http://lexvo.org/id/iso639-3/swe" in language_urls

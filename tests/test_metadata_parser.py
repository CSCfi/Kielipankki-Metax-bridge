import pytest
from lxml import etree
from harvester.metadata_parser import MSRecordParser


def _get_file_as_lxml(filename):
    """Returns given file as an lxml element."""
    with open(filename) as infile:
        return etree.fromstring(infile.read())


def test_get_title(basic_cmdi_record, kielipankki_datacatalog_id):
    """Testing that different language versions of "title" are mapped."""
    result = basic_cmdi_record.to_dict(data_catalog=kielipankki_datacatalog_id)["title"]
    expected_result = {
        "en": "Silva Kiuru's Time Expressions Corpus",
        "fi": "Silva Kiurun ajanilmausaineisto",
    }
    assert result == expected_result


def test_get_description(basic_cmdi_record, kielipankki_datacatalog_id):
    """Testing that different language versions of "description" are mapped."""
    result = basic_cmdi_record.to_dict(data_catalog=kielipankki_datacatalog_id)[
        "description"
    ]
    expected_result = {
        "en": "This corpus of time expressions has been compiled from literary works, translations, dialect texts as well as other texts. Format: word documents.",
        "fi": "Tämä suomen kielen ajanilmauksia käsittävä aineisto on koottu kaunokirjallisten alkuperäisteosten, käännösten, murreaineistojen ja muiden tekstien pohjalta.",
    }
    assert result == expected_result


def test_pid(basic_cmdi_record, dataset_pid):
    """Check that the correct PID is returned."""
    assert basic_cmdi_record.pid == dataset_pid


def test_get_modified_datetime(basic_cmdi_record, kielipankki_datacatalog_id):
    """Check that the modified date is returned in correct format."""
    result = basic_cmdi_record.to_dict(data_catalog=kielipankki_datacatalog_id)[
        "modified"
    ]
    expected_result = "2024-06-19T07:38:46Z"
    assert result == expected_result


def test_get_created_datetime(basic_cmdi_record, kielipankki_datacatalog_id):
    """Check that the created date is returned in correct format."""
    result = basic_cmdi_record.to_dict(data_catalog=kielipankki_datacatalog_id)[
        "created"
    ]
    expected_result = "2022-09-02T00:00:00Z"
    assert result == expected_result


def test_to_dict(basic_cmdi_record, dataset_pid, kielipankki_datacatalog_id):
    """Test that all relevant elements are mapped to a dictionary."""
    result = basic_cmdi_record.to_dict(data_catalog=kielipankki_datacatalog_id)
    expected_result = {
        "data_catalog": "urn:nbn:fi:att:data-catalog-kielipankki",
        "language": [{"url": "http://lexvo.org/id/iso639-3/fin"}],
        "field_of_science": [{"url": "http://www.yso.fi/onto/okm-tieteenala/ta6121"}],
        "persistent_identifier": dataset_pid,
        "title": {
            "en": "Silva Kiuru's Time Expressions Corpus",
            "fi": "Silva Kiurun ajanilmausaineisto",
        },
        "description": {
            "en": "This corpus of time expressions has been compiled from literary works, translations, dialect texts as well as other texts. Format: word documents.",
            "fi": "Tämä suomen kielen ajanilmauksia käsittävä aineisto on koottu kaunokirjallisten alkuperäisteosten, käännösten, murreaineistojen ja muiden tekstien pohjalta.",
        },
        "created": "2022-09-02T00:00:00Z",
        "modified": "2024-06-19T07:38:46Z",
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
                "person": {"email": "miina@example.com", "name": "Miina Metadataaja"},
                "roles": ["creator"],
                "organization": None,
            },
            {
                "roles": ["publisher", "rights_holder"],
                "organization": {
                    "url": "http://uri.suomi.fi/codelist/fairdata/organization/code/01901"
                },
            },
            {
                "roles": ["curator"],
                "person": {
                    "name": "Kiia Kontakti",
                    "email": "kiia@example.com",
                },
                "organization": {
                    "url": "http://uri.suomi.fi/codelist/fairdata/organization/code/01901"
                },
            },
        ],
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


def test_get_resource_languages(basic_cmdi_record):
    """
    Verify that a single language from a record is returned in Metax-approved format.

    Metax espects a list of dicts, each dict describing one language. The only
    information required for each language is a lexvo url.
    """
    assert basic_cmdi_record._get_resource_languages() == [
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


def test_get_actors(basic_cmdi_record):
    """
    Check that all actor data is present in a Metax-compatible format for a basic
    record.

    Publisher role does not yet contain meaningful information due to the test data only
    having an organization as a distributionRightsHolder and organizations have not been
    implemented yet.
    """
    actors = basic_cmdi_record._get_actors()

    assert len(actors) == 3

    assert {
        "person": {"email": "miina@example.com", "name": "Miina Metadataaja"},
        "roles": ["creator"],
        "organization": None,
    } in actors

    assert {
        "roles": ["curator"],
        "person": {"name": "Kiia Kontakti", "email": "kiia@example.com"},
        "organization": {
            "url": "http://uri.suomi.fi/codelist/fairdata/organization/code/01901"
        },
    } in actors

    assert {
        "roles": ["publisher", "rights_holder"],
        "organization": {
            "url": "http://uri.suomi.fi/codelist/fairdata/organization/code/01901"
        },
    } in actors


def test_multiple_actors_for_same_role():
    """
    Check that having more than one actor for a single role will report them all.
    """
    record = MSRecordParser(
        _get_file_as_lxml(
            "tests/test_data/kielipankki_record_sample_multiple_actors.xml"
        )
    )
    assert record._get_actors() == [
        {
            "roles": ["creator"],
            "person": {
                "name": "Miina Metadataattori",
                "email": "metadatamiina@example.com",
            },
            "organization": {
                "url": "http://uri.suomi.fi/codelist/fairdata/organization/code/10089"
            },
        },
        {
            "roles": ["creator", "rights_holder"],
            "person": {
                "name": "Aarne Aputoveri",
                "email": "aarne.aputoveri@example.com",
            },
            "organization": {
                "url": "http://uri.suomi.fi/codelist/fairdata/organization/code/01901"
            },
        },
        {
            "organization": {
                "email": "imaginary@example.com",
                "homepage": {"identifier": "http://www.imaginary.example"},
                "pref_label": {
                    "en": "Imaginary organization",
                    "fi": "Kuvitteellinen organisaatio",
                },
            },
            "person": {
                "email": "aaffil@example.com",
                "name": "Amanda Affiliaattori",
            },
            "roles": [
                "creator",
            ],
        },
        {
            "roles": ["publisher"],
            "organization": {
                "url": "http://uri.suomi.fi/codelist/fairdata/organization/code/01901",
            },
        },
        {
            "roles": ["curator"],
            "person": {
                "name": "User support FIN-CLARIN",
                "email": "fin-clarin@helsinki.fi",
            },
            "organization": None,
        },
        {
            "roles": ["rights_holder"],
            "person": {"name": "Tepi Tutkija", "email": "tepitutkija@example.com"},
            "organization": {
                "url": "http://uri.suomi.fi/codelist/fairdata/organization/code/01901",
            },
        },
    ]


def test_multiple_names_for_actor():
    """
    Check that having the name in more than one language will result in Finnish name
    being preferred.

    The test data for this test would also have the Finnish name "Kaarle Kustaa"
    available, but the English "Carl Gustaf" should be preferred.
    """
    record = MSRecordParser(
        _get_file_as_lxml(
            "tests/test_data/kielipankki_record_sample_actor_with_multiple_names.xml"
        )
    )
    actors = record._get_actors()
    assert len(actors) == 1
    assert actors[0]["person"]["name"] == "Carl Gustaf Bernadotte"


def test_publisher_person():
    """
    Check that personal information for a publisher is correctly extracted.

    This is not properly covered with the other test data, as it is more common that the
    publisher is an organization.
    """
    record = MSRecordParser(
        _get_file_as_lxml(
            "tests/test_data/kielipankki_record_sample_with_publisher_person.xml"
        )
    )
    actors = record.to_dict(data_catalog="catalog_id")["actors"]
    assert len(actors) == 1
    assert "publisher" in actors[0]["roles"]
    assert actors[0]["person"]["name"] == "Late Lisensoija"

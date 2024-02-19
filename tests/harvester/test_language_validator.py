import json
import pytest

from harvester import language_validator


@pytest.mark.usefixtures("mock_metax_language_vocabulary_endpoint")
def test_language_in_vocabulary(language_vocabulary_endpoint_url):
    """
    Test that languages and language families from the test response are really reported
    as valid, and languages not present in the test response are not valid.
    """
    assert language_validator.language_in_vocabulary(
        "http://lexvo.org/id/iso639-3/fin",
        language_vocabulary_endpoint=language_vocabulary_endpoint_url,
    )
    assert language_validator.language_in_vocabulary(
        "http://lexvo.org/id/iso639-5/smi",
        language_vocabulary_endpoint=language_vocabulary_endpoint_url,
    )
    assert not language_validator.language_in_vocabulary(
        "http://lexvo.org/id/iso639-3/åäö",
        language_vocabulary_endpoint=language_vocabulary_endpoint_url,
    )

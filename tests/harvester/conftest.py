import pytest

from harvester.pmh_interface import PMH_API


@pytest.fixture
def pmh_api(kielipankki_api_url):
    """
    PMH_API for running tests
    """
    return PMH_API("https://clarino.uib.no/oai")

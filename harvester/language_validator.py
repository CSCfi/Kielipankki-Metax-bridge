"""
Helpers for verifying that a provided language URI is accepted by Metax.
"""

import functools

import requests


@functools.lru_cache
def _allowed_language_uris(language_vocabulary_endpoint):
    """
    Return a list of allowed language URIs from the given endpoint.

    The HTTP request is made only once, consecutive calls get cached data.
    """
    metax_languages_response = requests.get(language_vocabulary_endpoint)
    metax_languages_response.raise_for_status()

    return {
        hit["_source"]["uri"] for hit in metax_languages_response.json()["hits"]["hits"]
    }


def language_in_vocabulary(
    language_uri,
    language_vocabulary_endpoint="https://metax.fairdata.fi/es/reference_data/language/_search?size=10000",
):
    """
    Check whether given language URI is listed in the vocabulary.

    The default vocabulary is that of production Metax.
    """
    return language_uri in _allowed_language_uris(language_vocabulary_endpoint)

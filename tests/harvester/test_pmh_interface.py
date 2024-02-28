import urllib


def test_fetch_records_with_last_harvest_date(
    pmh_api,
    shared_request_mocker,
    mock_metashare_get_single_record,
    latest_harvest_timestamp,
):
    """
    Ensure that fetching records from a given date produces the expected record and has
    used the given start date in the request.
    """
    records = [
        record.to_dict()
        for record in pmh_api.fetch_records(from_timestamp=latest_harvest_timestamp)
    ]
    assert records == mock_metashare_get_single_record
    assert (
        urllib.parse.quote(latest_harvest_timestamp)
        in shared_request_mocker.request_history[0].url
    )


def test_fetch_records_without_last_harvest_date(
    pmh_api,
    shared_request_mocker,
    mock_metashare_get_single_record,
    latest_harvest_timestamp,
):
    """
    Ensure that fetching records without a given start date produces the expected record
    and has not used the "from" parameter in the request.
    """
    records = [record.to_dict() for record in pmh_api.fetch_records()]
    assert records == mock_metashare_get_single_record
    assert "from=" not in shared_request_mocker.last_request.url


def test_corpus_pids_returns_correct_pids(pmh_api, mock_corpus_pid_list_from_metashare):
    """
    Verify that PMH_API is able to fetch PIDs for corpora.

    The test data has non-corpora resources too, but those must not be present in the
    returned PIDs.
    """
    assert pmh_api.corpus_pids == mock_corpus_pid_list_from_metashare

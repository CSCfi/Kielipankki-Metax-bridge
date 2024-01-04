def test_corpus_pids_returns_correct_pids(pmh_api, mock_corpus_pid_list_from_metashare):
    """
    Verify that PMH_API is able to fetch PIDs for corpora.

    The test data has non-corpora resources too, but those must not be present in the
    returned PIDs.
    """
    assert pmh_api.corpus_pids == mock_corpus_pid_list_from_metashare

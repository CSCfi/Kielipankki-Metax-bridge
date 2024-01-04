def test_pids(pmh_api, mock_pids_list_from_metashare):
    assert pmh_api.corpus_pids == mock_pids_list_from_metashare

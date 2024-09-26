from databricks.labs.ucx.progress.install import HistoryInstallation


def test_history_installation_run_creates_history_schema(mock_backend) -> None:
    installation = HistoryInstallation(mock_backend, "ucx")
    installation.run()
    assert mock_backend.queries[0] == "CREATE SCHEMA IF NOT EXISTS ucx.history"

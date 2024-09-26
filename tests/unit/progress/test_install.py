from databricks.labs.ucx.progress.install import HistoryInstallation


def test_history_installation_run_creates_history_schema(mock_backend) -> None:
    installation = HistoryInstallation(mock_backend, "ucx")
    installation.run()
    assert "CREATE SCHEMA IF NOT EXISTS ucx.history" in mock_backend.queries[0]


def test_history_installation_run_creates_workflow_runs_table(mock_backend) -> None:
    installation = HistoryInstallation(mock_backend, "ucx")
    installation.run()
    # Dataclass to schema conversion is tested within the lsql package
    assert any("CREATE TABLE IF NOT EXISTS" in query for query in mock_backend.queries)

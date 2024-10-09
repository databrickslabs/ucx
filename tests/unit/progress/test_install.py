from databricks.labs.ucx.progress.install import ProgressTrackingInstallation


def test_progress_tracking_installation_run_creates_progress_tracking_schema(mock_backend) -> None:
    installation = ProgressTrackingInstallation(mock_backend, "ucx")
    installation.run()
    assert "CREATE SCHEMA IF NOT EXISTS ucx.multiworkspace" in mock_backend.queries[0]


def test_progress_tracking_installation_run_creates_tables(mock_backend) -> None:
    installation = ProgressTrackingInstallation(mock_backend, "ucx")
    installation.run()
    # Dataclass to schema conversion is tested within the lsql package
    assert sum("CREATE TABLE IF NOT EXISTS" in query for query in mock_backend.queries) == 2

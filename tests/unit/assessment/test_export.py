from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.assessment.export import AssessmentExporter
from databricks.labs.lsql.backends import MockBackend
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.lsql.core import Row


def test_export(tmp_path):
    """Test the export_results method of the AssessmentExporter class."""
    query = {
        "SELECT\n  one\nFROM ucx.external_locations": [
            Row(location="s3://bucket1/folder1", table_count=1),
            Row(location="abfss://container1@storage1.dfs.core.windows.net/folder1", table_count=1),
            Row(location="gcp://folder1", table_count=2),
        ]
    }

    # Setup workspace configuration
    config = WorkspaceConfig(inventory_database="ucx")

    # Prepare temporary paths and files
    export_path = tmp_path / "export"
    export_path.mkdir(parents=True, exist_ok=True)

    # Mock backend and prompts
    mock_backend = MockBackend(rows=query)
    query_choice = {"assessment_name": "main", "option": 3}
    mock_prompts = MockPrompts(
        {
            "Choose a path to save the UCX Assessment results": export_path.as_posix(),
            "Choose which assessment results to export": query_choice["option"],
        }
    )

    # Execute export process
    export = AssessmentExporter(mock_backend, config)
    exported = export.export_results(mock_prompts)

    # Assertion based on the query_choice
    expected_file_name = f"export_{query_choice['assessment_name']}_results.zip"  # Adjusted filename
    assert exported == export_path / expected_file_name

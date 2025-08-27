from unittest.mock import create_autospec, patch


from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.assessment.export import AssessmentExporter
from databricks.labs.lsql.backends import MockBackend
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.lsql.core import Row


QUERY = {
    "SELECT\n  one\nFROM ucx.external_locations": [
        Row(location="s3://bucket1/folder1", table_count=1),
        Row(location="abfss://container1@storage1.dfs.core.windows.net/folder1", table_count=1),
        Row(location="gcp://folder1", table_count=2),
    ]
}

CONFIG = WorkspaceConfig(inventory_database="ucx")


def test_cli_csv_export(ws, tmp_path):
    """Test the cli csv export_results method of the AssessmentExporter class."""

    # Prepare temporary paths and files
    export_path = tmp_path / "export_csv"
    export_path.mkdir(parents=True, exist_ok=True)

    # Mock backend and prompts
    mock_backend = MockBackend(rows=QUERY)
    query_choice = {"assessment_name": "main", "option": 3}
    mock_prompts = MockPrompts(
        {
            "Choose a path to save the UCX Assessment results": export_path.as_posix(),
            "Choose which assessment results to export": query_choice["option"],
        }
    )

    # Execute export process
    export = AssessmentExporter(ws, mock_backend, CONFIG)
    exported = export.cli_export_csv_results(mock_prompts)

    # Assertion based on the query_choice
    expected_file_name = f"export_{query_choice['assessment_name']}_results.zip"
    assert exported == export_path / expected_file_name


def test_cli_xlsx_export(ws, tmp_path):
    """Test the cli xlsx export_results method of the AssessmentExporter class."""

    # Prepare temporary paths and files
    export_path = tmp_path / "export_excel"
    export_path.mkdir(parents=True, exist_ok=True)

    # Mock backend and prompts
    mock_backend = MockBackend(rows=QUERY)
    mock_prompts = MockPrompts(
        {
            "Choose a path to save the UCX Assessment results": export_path.as_posix(),
        }
    )

    # Execute export process
    export = AssessmentExporter(ws, mock_backend, CONFIG)
    exported = export.cli_export_xlsx_results(mock_prompts)

    # Assertion based on the query_choice
    expected_file_name = "ucx_assessment_main.xlsx"  # Adjusted filename
    assert exported == export_path / expected_file_name


def test_web_export(ws, tmp_path):
    """Test the web export_results method of the AssessmentExporter class."""
    config = WorkspaceConfig(inventory_database="ucx")
    mock_backend = MockBackend()

    export = AssessmentExporter(ws, mock_backend, config)
    expected_html = """
            <div class="export-container">
                <h2>Export Results</h2>
                <button onclick="downloadExcel()">Download Results</button>
            </div>
            """

    class ExcelWriterSpec:
        def __init__(self, path, engine=None, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

        def close(self):
            pass

        def save(self):
            pass

    class PandasSpec:
        ExcelWriter = ExcelWriterSpec

    mock_writer = create_autospec(PandasSpec)

    with patch.object(export, '_render_export') as mock_render_export:
        mock_render_export.return_value = expected_html

        result = export.web_export_results(mock_writer)
        assert "downloadExcel()" in result
        assert "Download Results" in result

    mock_writer.ExcelWriter.assert_called_once()

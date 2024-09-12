import logging
from pathlib import Path

from databricks.labs.blueprint.tui import Prompts

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.lsql.dashboards import DashboardMetadata

logger = logging.getLogger(__name__)


class AssessmentExporter:
    # File and Path Constants
    _EXPORT_FILE_NAME = "ucx_assessment_results.zip"

    def __init__(self, sql_backend: SqlBackend, config: WorkspaceConfig):
        self._sql_backend = sql_backend
        self._config = config

    def export_results(self, prompts: Prompts):
        """Main method to export results to CSV files inside a ZIP archive."""
        project_root = Path(__file__).resolve().parents[3]
        queries_path_root = project_root / f"labs/ucx/queries/assessment"
        valid_queries = {subdir.name for subdir in queries_path_root.iterdir() if subdir.is_dir()}

        export_path = Path(
            prompts.question(
                "Choose a path to save the UCX Assessment results",
                default=Path.cwd().as_posix(),
                validate=lambda p_: Path(p_).exists(),
            )
        )

        query_choice = prompts.question(
            "Choose which assessment results to export",
            default="main",
            validate=lambda q: q in valid_queries,
        )


        # Initialize and export results
        queries_path = queries_path_root / query_choice
        dashboard_metadata = DashboardMetadata(display_name="UCX Assessment Results")
        assessment_results = dashboard_metadata.from_path(queries_path)
        assessment_results = assessment_results.replace_database(
            catalog="hive_metastore", database=self._config.inventory_database
        )

        # Export results
        logger.info("Exporting assessment results....")
        results_path = assessment_results.export_to_zipped_csv(self._sql_backend, export_path)
        logger.info(f"Results exported to {results_path}")

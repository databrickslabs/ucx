import logging
from pathlib import Path

from databricks.labs.blueprint.tui import Prompts

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.lsql.dashboards import DashboardMetadata

logger = logging.getLogger(__name__)


class AssessmentExporter:

    def __init__(self, sql_backend: SqlBackend, config: WorkspaceConfig):
        self._sql_backend = sql_backend
        self._config = config

    def export_results(self, prompts: Prompts):
        """Main method to export results to CSV files inside a ZIP archive."""
        project_root = Path(__file__).resolve().parents[3]
        queries_path_root = project_root / "labs/ucx/queries/assessment"

        results_directory = Path(
            prompts.question(
                "Choose a path to save the UCX Assessment results",
                default=Path.cwd().as_posix(),
                validate=lambda p_: Path(p_).exists(),
            )
        )

        query_choice = prompts.choice(
            "Choose which assessment results to export",
            [subdir.name for subdir in queries_path_root.iterdir() if subdir.is_dir()],
        )

        export_path = results_directory / f"export_{query_choice}_results.zip"
        queries_path = queries_path_root / query_choice

        assessment_results = DashboardMetadata.from_path(queries_path).replace_database(
            database=self._config.inventory_database, database_to_replace="inventory"
        )

        logger.info("Exporting assessment results....")
        results_path = assessment_results.export_to_zipped_csv(self._sql_backend, export_path)
        logger.info(f"Results exported to {results_path}")

        return results_path

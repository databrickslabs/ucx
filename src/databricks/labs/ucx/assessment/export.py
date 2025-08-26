import base64
import logging
from datetime import timedelta
from pathlib import Path
from typing import Any

from databricks.sdk.service import compute, jobs
from databricks.sdk.service.jobs import RunResultState
from databricks.sdk.service.workspace import ExportFormat
from databricks.sdk.errors import NotFound, ResourceDoesNotExist
from databricks.sdk.retries import retried
from databricks.sdk import WorkspaceClient

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import Prompts
from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.lsql.dashboards import DashboardMetadata

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.assessment.export_html_template import EXPORT_HTML_TEMPLATE

logger = logging.getLogger(__name__)


class AssessmentExporter:

    def __init__(self, ws: WorkspaceClient, sql_backend: SqlBackend, config: WorkspaceConfig):
        self._ws = ws
        self._sql_backend = sql_backend
        self._config = config
        self._install_folder = f"/Workspace/{Installation.assume_global(ws, 'ucx')}/"
        self._base_path = Path(__file__).resolve().parents[3] / "labs/ucx/queries/assessment"

    @staticmethod
    def _export_to_excel(
        assessment_metadata: DashboardMetadata, sql_backend: SqlBackend, export_path: Path, writter: Any
    ):
        """Export Assessment to Excel"""
        with writter.ExcelWriter(export_path, engine='xlsxwriter') as writer:
            for tile in assessment_metadata.tiles:
                if not tile.metadata.is_query():
                    continue

                try:
                    rows = list(sql_backend.fetch(tile.content))
                    if not rows:
                        continue

                    data = [row.asDict() for row in rows]
                    df = writter.DataFrame(data)

                    sheet_name = str(tile.metadata.id)[:31]
                    df.to_excel(writer, sheet_name=sheet_name, index=False)

                except NotFound as e:
                    msg = (
                        str(e).split(" Verify", maxsplit=1)[0] + f" Export will continue without {tile.metadata.title}"
                    )
                    logging.warning(msg)
                    continue

    @retried(on=[ResourceDoesNotExist], timeout=timedelta(minutes=1))
    def _render_export(self, export_file_path: Path) -> str:
        """Render an HTML link for downloading the results."""
        binary_data = self._ws.workspace.download(export_file_path.as_posix()).read()
        b64_data = base64.b64encode(binary_data).decode('utf-8')

        return EXPORT_HTML_TEMPLATE.format(b64_data=b64_data, export_file_path_name=export_file_path.name)

    @staticmethod
    def _get_output_directory(prompts: Prompts) -> Path:
        return Path(
            prompts.question(
                "Choose a path to save the UCX Assessment results",
                default=Path.cwd().as_posix(),
                validate=lambda p_: Path(p_).exists(),
            )
        )

    def _get_queries(self, assessment: str) -> DashboardMetadata:
        """Get UCX queries to export"""
        queries_path = self._base_path / assessment if assessment else self._base_path
        return DashboardMetadata.from_path(queries_path).replace_database(
            database=self._config.inventory_database, database_to_replace="inventory"
        )

    def cli_export_csv_results(self, prompts: Prompts) -> Path:
        """Main method to export results to CSV files inside a ZIP archive."""
        results_directory = self._get_output_directory(prompts)

        query_choice = prompts.choice(
            "Choose which assessment results to export",
            [subdir.name for subdir in self._base_path.iterdir() if subdir.is_dir()],
        )

        results_path = self._get_queries(query_choice).export_to_zipped_csv(
            self._sql_backend, results_directory / f"export_{query_choice}_results.zip"
        )

        return results_path

    def cli_export_xlsx_results(self, prompts: Prompts) -> Path:
        """Submit Excel export notebook in a job"""

        notebook_path = f"{self._install_folder}/EXPORT_ASSESSMENT_TO_EXCEL"
        export_file_name = Path(f"{self._install_folder}/ucx_assessment_main.xlsx")
        results_directory = Path(self._get_output_directory(prompts)) / export_file_name.name

        run = self._ws.jobs.submit_and_wait(
            run_name="export-assessment-to-excel-experimental",
            tasks=[
                jobs.SubmitTask(
                    notebook_task=jobs.NotebookTask(notebook_path=notebook_path),
                    task_key="export-assessment",
                    new_cluster=compute.ClusterSpec(
                        data_security_mode=compute.DataSecurityMode.LEGACY_SINGLE_USER_STANDARD,
                        spark_conf={
                            "spark.databricks.cluster.profile": "singleNode",
                            "spark.master": "local[*]",
                        },
                        custom_tags={"ResourceClass": "SingleNode"},
                        num_workers=0,
                        policy_id=self._config.policy_id,
                        apply_policy_default_values=True,
                    ),
                )
            ],
        )

        if run.state and run.state.result_state == RunResultState.SUCCESS:
            binary_resp = self._ws.workspace.download(path=export_file_name.as_posix(), format=ExportFormat.SOURCE)
            results_directory.write_bytes(binary_resp.read())

        return results_directory

    def web_export_results(self, writer: Any) -> str:
        """Alternative method to export results from the UI."""
        export_file_name = Path(f"{self._install_folder}/ucx_assessment_main.xlsx")
        assessment_main = self._get_queries("main")
        self._export_to_excel(assessment_main, self._sql_backend, export_file_name, writer)
        return self._render_export(export_file_name)

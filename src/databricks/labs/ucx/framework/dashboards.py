import logging
from collections.abc import Callable
from pathlib import Path

import sqlglot
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.lsql.dashboards import Dashboards
from databricks.sdk import WorkspaceClient


logger = logging.getLogger(__name__)


class DashboardFromFiles:
    def __init__(
        self,
        ws: WorkspaceClient,
        state: InstallState,
        local_folder: Path,
        remote_folder: str,
        name_prefix: str,
        query_transformer: Callable[[sqlglot.Expression], sqlglot.Expression] | None = None,
        warehouse_id: str | None = None,
    ):
        self._ws = ws
        self._state = state
        self._local_folder = local_folder
        self._remote_folder = remote_folder
        self._name_prefix = name_prefix
        self._query_transformer = query_transformer
        self._warehouse_id = warehouse_id

        self._dashboards = Dashboards(self._ws)

    def dashboard_link(self, dashboard_ref: str):
        dashboard_id = self._state.dashboards[dashboard_ref]
        dashboard_url = f"{self._ws.config.host}/sql/dashboardsv3/{dashboard_id}"
        return dashboard_url

    def create_dashboards(self) -> None:
        # Iterate over dashboards for each step, represented as first-level folders
        step_folders = [p for p in self._local_folder.iterdir() if p.is_dir()]
        for step_folder in step_folders:
            logger.debug(f"Reading step folder {step_folder}...")
            dashboard_folders = [p for p in step_folder.iterdir() if p.is_dir()]
            # Create separate dashboards per step, represented as second-level folders
            for dashboard_folder in dashboard_folders:
                logger.info(f"Creating dashboard in {dashboard_folder}...")
                lakeview_dashboard = self._dashboards.create_dashboard(
                    dashboard_folder, query_transformer=self._query_transformer
                )
                main_name = step_folder.stem.title()
                sub_name = dashboard_folder.stem.title()
                dashboard_name = f"{self._name_prefix} {main_name} ({sub_name})"
                lakeview_dashboard.pages[0].display_name = dashboard_name
                dashboard_ref = f"{step_folder.stem}_{dashboard_folder.stem}".lower()
                dashboard_id = self._state.dashboards.get(dashboard_ref)
                dashboard = self._dashboards.deploy_dashboard(
                    lakeview_dashboard,
                    dashboard_id=dashboard_id,
                    parent_path=self._remote_folder,
                    warehouse_id=self._warehouse_id,
                )
                assert dashboard.dashboard_id is not None
                self._ws.lakeview.publish(dashboard.dashboard_id)
                self._state.dashboards[dashboard_ref] = dashboard.dashboard_id

    def validate(self):
        step_folders = [p for p in self._local_folder.iterdir() if p.is_dir()]
        for step_folder in step_folders:
            logger.info(f"Reading step folder {step_folder}...")
            dashboard_folders = [p for p in step_folder.iterdir() if p.is_dir()]
            # Create separate dashboards per step, represented as second-level folders
            for dashboard_folder in dashboard_folders:
                self._validate_folder(dashboard_folder)

    def _validate_folder(self, folder: Path):
        try:
            dashboard = self._dashboards.create_dashboard(folder)
        except ValueError as e:
            raise AssertionError(f"Creating dashboard in {folder}") from e
        if len(dashboard.datasets) != len(list(folder.glob("*.sql"))):
            raise AssertionError(f"Dashboard in {folder} contains invalid query.")

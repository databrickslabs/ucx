import io
import json

import yaml
from databricks.sdk.service import iam
from databricks.sdk.service.sql import (
    Dashboard,
    DataSource,
    Query,
    Visualization,
    Widget,
)

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.dashboards import DashboardFromFiles
from databricks.labs.ucx.framework.install_state import InstallState
from databricks.labs.ucx.framework.wheels import find_project_root
from databricks.labs.ucx.install import WorkspaceInstaller


def test_dashboard(mocker):
    ws = mocker.Mock()
    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    ws.config.host = "https://foo"
    ws.config.is_aws = True
    install_folder = "/users/not_a_real_user"
    queries_folder = f"{install_folder}/queries"

    def download_mock(path):
        if path == f"{queries_folder}/state.json":
            return io.StringIO(
                json.dumps(
                    {
                        "jobs.sql:query_id": "91e51760-7653-4769-bc32-1595ce1892af",
                        "all_tables.sql:query_id": "4000e54c-4c51-45b3-b009-a4dd9a3b5599",
                    }
                )
            )

        if path == f"{install_folder}/state.json":
            return io.StringIO(
                json.dumps(
                    {
                        "$version": 1,
                        "resources": {
                            "queries": {
                                "jobs.sql": "91e51760-7653-4769-bc32-1595ce1892af",
                                "all_tables.sql": "4000e54c-4c51-45b3-b009-a4dd9a3b5599",
                            }
                        },
                    }
                )
            )

        config_bytes = yaml.dump(WorkspaceConfig(inventory_database="a").as_dict()).encode("utf8")
        return io.BytesIO(config_bytes)

    ws.workspace.download = download_mock
    ws.data_sources.list = lambda: [DataSource(id="bcd", warehouse_id="000000")]
    ws.dashboards.create.return_value = Dashboard(id="abc")
    ws.queries.create.return_value = Query(id="abc")
    ws.query_visualizations.create.return_value = Visualization(id="abc")
    ws.dashboard_widgets.create.return_value = Widget(id="abc")
    ws.warehouses.list.return_value = []
    installer = WorkspaceInstaller(ws)
    local_query_files = find_project_root() / "src/databricks/labs/ucx/queries"
    dash = DashboardFromFiles(
        ws,
        InstallState(ws, install_folder),
        local_folder=local_query_files,
        remote_folder=queries_folder,
        name_prefix="Assessment",
        warehouse_id="000000",
        query_text_callback=installer.current_config.replace_inventory_variable,
    )
    dashboards = dash.create_dashboards()
    assert dashboards is not None
    assert dashboards["assessment_main"] == "abc"
    assert dashboards["assessment_azure"] == "abc"

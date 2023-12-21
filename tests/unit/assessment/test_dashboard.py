import io

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
    config_bytes = yaml.dump(WorkspaceConfig(inventory_database="a").as_dict()).encode("utf8")
    ws.workspace.download = lambda _: io.BytesIO(config_bytes)
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
        InstallState(ws, "/users/not_a_real_user"),
        local_folder=local_query_files,
        remote_folder="/users/not_a_real_user/queries",
        name_prefix="Assessment",
        warehouse_id="000000",
        query_text_callback=installer.current_config.replace_inventory_variable,
    )
    dashboards = dash.create_dashboards()
    assert dashboards is not None
    assert dashboards["assessment_main"] == "abc"
    assert dashboards["assessment_azure"] == "abc"

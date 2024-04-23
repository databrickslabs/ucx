from unittest.mock import create_autospec

from databricks.labs.blueprint.entrypoint import find_project_root
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.installer import InstallState
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam
from databricks.sdk.service.sql import (
    Dashboard,
    DataSource,
    Query,
    Visualization,
    Widget,
)

from databricks.labs.ucx.framework.dashboards import DashboardFromFiles


def test_dashboard():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    ws.config.host = "https://foo"
    ws.config.is_aws = True
    install_folder = "/users/not_a_real_user"
    queries_folder = f"{install_folder}/queries"

    installation = MockInstallation(
        {
            'state.json': {
                "version": 1,
                "resources": {
                    "queries": {
                        "jobs.sql": "91e51760-7653-4769-bc32-1595ce1892af",
                        "all_tables.sql": "4000e54c-4c51-45b3-b009-a4dd9a3b5599",
                        "assessment_azure_05_0_azure_service_principals.sql": "_",
                    }
                },
            },
            'config.yml': {
                'version': 2,
                'inventory_database': 'a',
            },
        }
    )

    ws.data_sources.list = lambda: [DataSource(id="bcd", warehouse_id="000000")]
    ws.dashboards.create.return_value = Dashboard(id="abc")
    ws.queries.create.return_value = Query(id="abc")
    ws.query_visualizations.create.return_value = Visualization(id="abc")
    ws.dashboard_widgets.create.return_value = Widget(id="abc")
    ws.warehouses.list.return_value = []
    local_query_files = find_project_root(__file__) / "src/databricks/labs/ucx/queries"
    dash = DashboardFromFiles(
        ws,
        InstallState(None, None, installation=installation),
        local_folder=local_query_files,
        remote_folder=queries_folder,
        name_prefix="Assessment",
        warehouse_id="000000",
        query_text_callback=lambda x: x,
    )
    dashboards = dash.create_dashboards()
    assert dashboards is not None
    assert dashboards["assessment_main"] == "abc"
    assert dashboards["assessment_azure"] == "abc"

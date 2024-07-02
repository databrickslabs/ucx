from unittest.mock import create_autospec

from databricks.labs.blueprint.entrypoint import find_project_root
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.installer import InstallState
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.framework.dashboards import DashboardFromFiles


def test_dashboard():
    ws = create_autospec(WorkspaceClient)
    installation = MockInstallation()
    local_query_files = find_project_root(__file__) / "src/databricks/labs/ucx/queries"
    dash = DashboardFromFiles(
        ws,
        InstallState(None, None, installation=installation),
        local_folder=local_query_files,
        remote_folder="/non/existing/folder/",
        name_prefix="Assessment",
        warehouse_id="000000",
        query_transformer=lambda x: x,
    )
    dash.create_dashboards()
    ws.lakeview.create.assert_called()

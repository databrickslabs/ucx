from databricks.labs.blueprint.entrypoint import find_project_root
from databricks.labs.blueprint.installer import InstallState
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.framework.dashboards import DashboardFromFiles
from databricks.labs.ucx.install import WorkspaceInstaller


# @pytest.mark.skip("not working")
def test_lvdash():
    # ws = WorkspaceClient(profile='e2-demo-field-eng')
    ws = WorkspaceClient(profile='logfood-master')
    installer = WorkspaceInstaller(ws)
    local_query_files = find_project_root(__file__) / "src/databricks/labs/ucx/queries"
    install_state = InstallState(ws, product='ucx')
    dash = DashboardFromFiles(
        ws,
        install_state,
        local_folder=local_query_files,
        remote_folder=install_state.install_folder(),
        name_prefix="Assessment",
        warehouse_id="000000",
        query_text_callback=installer.current_config.replace_inventory_variable,
    )
    dashboards = dash.create_lakeview()
    assert dashboards is not None

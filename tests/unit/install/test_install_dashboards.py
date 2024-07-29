import logging
from datetime import timedelta

import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.wheels import ProductInfo, WheelsV2, find_project_root
from databricks.labs.lsql.backends import MockBackend
from databricks.labs.lsql.dashboards import DashboardMetadata
from databricks.sdk.errors.platform import BadRequest
from databricks.sdk.service.dashboards import Dashboard as LakeviewDashboard, LifecycleState
from databricks.sdk.errors import NotFound, InvalidParameterValue

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.install import WorkspaceInstallation
from databricks.labs.ucx.installer.workflows import WorkflowsDeployment


PRODUCT_INFO = ProductInfo.from_class(WorkspaceConfig)


@pytest.fixture
def workspace_installation(request, ws, any_prompt) -> WorkspaceInstallation:
    mock_installation = request.param if hasattr(request, "param") else MockInstallation()
    install_state = InstallState.from_installation(mock_installation)
    wheels = WheelsV2(mock_installation, PRODUCT_INFO)
    workflows_installation = WorkflowsDeployment(
        WorkspaceConfig(inventory_database="...", policy_id='123'),
        mock_installation,
        install_state,
        ws,
        wheels,
        PRODUCT_INFO,
        timedelta(seconds=1),
        [],
    )
    workspace_installation = WorkspaceInstallation(
        WorkspaceConfig(inventory_database='ucx'),
        mock_installation,
        install_state,
        MockBackend(),
        ws,
        workflows_installation,
        any_prompt,
        PRODUCT_INFO,
    )
    return workspace_installation


def test_installation_creates_dashboard(ws, workspace_installation):
    workspace_installation.run()
    ws.lakeview.create.assert_called()


def test_installation_updates_dashboard(ws, workspace_installation):
    workspace_installation.run()
    ws.lakeview.update.assert_not_called()
    workspace_installation.run()
    ws.lakeview.update.assert_called()


@pytest.mark.parametrize(
    "workspace_installation",
    [MockInstallation({'state.json': {'resources': {'dashboards': {"assessment_main": "redash-id"}}}})],
    indirect=True,
)
def test_installation_upgrades_redash_dashboard(caplog, ws, workspace_installation):
    with caplog.at_level(logging.INFO, logger="databricks.labs.ucx.install"):
        workspace_installation.run()
    assert "Upgrading dashboard to Lakeview" in caplog.text
    ws.dashboards.delete.assert_called_once()


@pytest.mark.parametrize(
    "workspace_installation",
    [MockInstallation({'state.json': {'resources': {'dashboards': {"assessment_main": "redash-id"}}}})],
    indirect=True,
)
def test_installation_upgrades_non_existing_redash_dashboard(caplog, ws, workspace_installation):
    ws.dashboards.delete.side_effect = BadRequest
    with caplog.at_level(logging.INFO, logger="databricks.labs.ucx.install"):
        workspace_installation.run()
    assert "Upgrading dashboard to Lakeview" in caplog.text
    assert "Cannot delete dashboard" in caplog.text
    ws.dashboards.delete.assert_called_once()


@pytest.mark.parametrize(
    "workspace_installation",
    [MockInstallation({'state.json': {'resources': {'dashboards': {"assessment_main": "lakeview_id"}}}})],
    indirect=True,
)
def test_installation_updates_existing_lakeview_dashboard(ws, workspace_installation):
    ws.lakeview.get.return_value = LakeviewDashboard(lifecycle_state=LifecycleState.ACTIVE)
    workspace_installation.run()
    ws.lakeview.get.assert_called_with("lakeview_id")
    ws.lakeview.update.assert_called_once()


@pytest.mark.parametrize(
    "workspace_installation",
    [MockInstallation({'state.json': {'resources': {'dashboards': {"assessment_main": "lakeview_id"}}}})],
    indirect=True,
)
def test_installation_recreates_lakeview_dashboard_without_lifecycle_state(caplog, ws, workspace_installation):
    ws.lakeview.get.return_value = LakeviewDashboard(lifecycle_state=None)
    with caplog.at_level(logging.INFO, logger="databricks.labs.ucx.install"):
        workspace_installation.run()
    assert "Recovering invalid dashboard" in caplog.text
    ws.lakeview.create.assert_called()
    ws.lakeview.update.assert_not_called()


@pytest.mark.parametrize(
    "workspace_installation",
    [MockInstallation({'state.json': {'resources': {'dashboards': {"assessment_main": "lakeview_id"}}}})],
    indirect=True,
)
def test_installation_recreates_trashed_lakeview_dashboard(caplog, ws, workspace_installation):
    ws.lakeview.get.return_value = LakeviewDashboard(lifecycle_state=LifecycleState.TRASHED)
    with caplog.at_level(logging.INFO, logger="databricks.labs.ucx.install"):
        workspace_installation.run()
    assert "Recreating trashed dashboard" in caplog.text
    ws.lakeview.create.assert_called()
    ws.lakeview.update.assert_not_called()


@pytest.mark.parametrize(
    "workspace_installation",
    [MockInstallation({'state.json': {'resources': {'dashboards': {"assessment_main": "lakeview_id"}}}})],
    indirect=True,
)
@pytest.mark.parametrize("exception", [NotFound, InvalidParameterValue])
def test_installation_recovers_invalid_dashboard(caplog, ws, workspace_installation, exception):
    ws.lakeview.get.side_effect = exception
    with caplog.at_level(logging.DEBUG, logger="databricks.labs.ucx.install"):
        workspace_installation.run()
    assert "Recovering invalid dashboard" in caplog.text
    assert "Deleted dangling dashboard" in caplog.text
    ws.workspace.delete.assert_called_once()
    ws.lakeview.create.assert_called()
    ws.lakeview.update.assert_not_called()


@pytest.mark.parametrize(
    "workspace_installation",
    [MockInstallation({'state.json': {'resources': {'dashboards': {"assessment_main": "lakeview_id"}}}})],
    indirect=True,
)
def test_installation_recovers_invalid_deleted_dashboard(caplog, ws, workspace_installation):
    ws.lakeview.get.side_effect = NotFound
    ws.workspace.delete.side_effect = NotFound
    with caplog.at_level(logging.DEBUG, logger="databricks.labs.ucx.install"):
        workspace_installation.run()
    assert "Recovering invalid dashboard" in caplog.text
    assert "Deleted dangling dashboard" not in caplog.text
    ws.workspace.delete.assert_called_once()
    ws.lakeview.create.assert_called()
    ws.lakeview.update.assert_not_called()


def test_validate_dashboards(ws):
    queries_path = find_project_root(__file__) / "src/databricks/labs/ucx/queries"
    for step_folder in queries_path.iterdir():
        if not step_folder.is_dir():
            continue
        for dashboard_folder in step_folder.iterdir():
            if not dashboard_folder.is_dir():
                continue
            try:
                DashboardMetadata.from_path(dashboard_folder).validate()
            except ValueError as e:
                assert False, f"Invalid dashboard in {dashboard_folder}: {e}"
            else:
                assert True, f"Valid dashboard in {dashboard_folder}"

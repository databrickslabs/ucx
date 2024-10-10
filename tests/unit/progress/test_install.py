from unittest.mock import create_autospec

import pytest
from databricks.labs.blueprint.installer import InstallState
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service.catalog import CatalogInfo, MetastoreAssignment
from databricks.sdk.service.jobs import Run, RunResultState, RunState

from databricks.labs.ucx.hive_metastore.verification import VerifyHasCatalog, VerifyHasMetastore
from databricks.labs.ucx.installer.workflows import DeployedWorkflows
from databricks.labs.ucx.progress.install import ProgressTrackingInstallation, VerifyProgressTracking


def test_progress_tracking_installation_run_creates_progress_tracking_schema(mock_backend) -> None:
    installation = ProgressTrackingInstallation(mock_backend, "ucx")
    installation.run()
    assert "CREATE SCHEMA IF NOT EXISTS ucx.multiworkspace" in mock_backend.queries[0]


def test_progress_tracking_installation_run_creates_tables(mock_backend) -> None:
    installation = ProgressTrackingInstallation(mock_backend, "ucx")
    installation.run()
    # Dataclass to schema conversion is tested within the lsql package
    assert sum("CREATE TABLE IF NOT EXISTS" in query for query in mock_backend.queries) == 2


def test_verify_progress_tracking_valid_prerequisites(mock_installation) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.metastores.current.return_value = MetastoreAssignment(metastore_id="test", workspace_id=123456789)
    ws.catalogs.get.return_value = CatalogInfo()
    ws.jobs.list_runs.return_value = [Run(state=RunState(result_state=RunResultState.SUCCESS))]
    verify_progress_tracking = VerifyProgressTracking(
        VerifyHasMetastore(ws),
        VerifyHasCatalog(ws, "ucx"),
        DeployedWorkflows(ws, InstallState.from_installation(mock_installation)),
    )
    try:
        verify_progress_tracking.verify()
    except RuntimeError as e:
        assert False, f"Verify progress tracking raises: {e}"
    else:
        assert True, "Valid prerequisites found"


def test_verify_progress_tracking_raises_runtime_error_if_metastore_not_attached_to_workflow(mock_installation) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.metastores.current.return_value = None
    verify_progress_tracking = VerifyProgressTracking(
        VerifyHasMetastore(ws),
        VerifyHasCatalog(ws, "ucx"),
        DeployedWorkflows(ws, InstallState.from_installation(mock_installation)),
    )
    with pytest.raises(RuntimeWarning, match="Metastore not attached to workspace"):
        verify_progress_tracking.verify()


def test_verify_progress_tracking_raises_runtime_error_if_missing_permissions_to_access_metastore(
    mock_installation,
) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.metastores.current.side_effect = PermissionDenied
    verify_progress_tracking = VerifyProgressTracking(
        VerifyHasMetastore(ws),
        VerifyHasCatalog(ws, "ucx"),
        DeployedWorkflows(ws, InstallState.from_installation(mock_installation)),
    )
    with pytest.raises(RuntimeWarning, match="Metastore not attached to workspace"):
        verify_progress_tracking.verify()


def test_verify_progress_tracking_raises_runtime_error_if_missing_ucx_catalog(mock_installation) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.catalogs.get.return_value = None
    verify_progress_tracking = VerifyProgressTracking(
        VerifyHasMetastore(ws),
        VerifyHasCatalog(ws, "ucx"),
        DeployedWorkflows(ws, InstallState.from_installation(mock_installation)),
    )
    with pytest.raises(RuntimeWarning, match="UCX catalog not configured."):
        verify_progress_tracking.verify()


def test_verify_progress_tracking_raises_runtime_error_if_missing_permissions_to_access_ucx_catalog(
    mock_installation,
) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.catalogs.get.side_effect = PermissionDenied
    verify_progress_tracking = VerifyProgressTracking(
        VerifyHasMetastore(ws),
        VerifyHasCatalog(ws, "ucx"),
        DeployedWorkflows(ws, InstallState.from_installation(mock_installation)),
    )
    with pytest.raises(RuntimeWarning, match="UCX catalog not configured."):
        verify_progress_tracking.verify()


def test_verify_progress_tracking_raises_runtime_error_if_assessment_workflow_did_not_run(mock_installation) -> None:
    ws = create_autospec(WorkspaceClient)
    verify_progress_tracking = VerifyProgressTracking(
        VerifyHasMetastore(ws),
        VerifyHasCatalog(ws, "ucx"),
        DeployedWorkflows(ws, InstallState.from_installation(mock_installation)),
    )
    with pytest.raises(RuntimeWarning, match="Assessment workflow did not complete successfully yet."):
        verify_progress_tracking.verify()

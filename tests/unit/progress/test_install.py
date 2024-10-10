import datetime as dt
from unittest.mock import create_autospec

import pytest
from databricks.labs.blueprint.installer import InstallState
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied

from databricks.labs.ucx.hive_metastore.verification import MetastoreNotFoundError, VerifyHasCatalog, VerifyHasMetastore
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


def test_verify_progress_tracking_valid_prerequisites() -> None:
    verify_has_metastore = create_autospec(VerifyHasMetastore)
    verify_has_catalog = create_autospec(VerifyHasCatalog)
    deployed_workflows = create_autospec(DeployedWorkflows)
    verify_progress_tracking = VerifyProgressTracking(verify_has_metastore, verify_has_catalog, deployed_workflows)
    timeout = dt.timedelta(hours=1)
    try:
        verify_progress_tracking.verify(timeout=timeout)
    except RuntimeError as e:
        assert False, f"Verify progress tracking raises: {e}"
    else:
        assert True, "Valid prerequisites found"
    verify_has_metastore.verify_metastore.assert_called_once()
    verify_has_catalog.verify.assert_called_once()
    deployed_workflows.validate_step.assert_called_once_with("assessment", timeout=timeout)


def test_verify_progress_tracking_raises_runtime_error_if_metastore_not_attached_to_workflow(mock_installation) -> None:
    verify_has_metastore = create_autospec(VerifyHasMetastore)
    verify_has_metastore.verify_metastore.side_effect = MetastoreNotFoundError
    verify_has_catalog = create_autospec(VerifyHasCatalog)
    deployed_workflows = create_autospec(DeployedWorkflows)
    verify_progress_tracking = VerifyProgressTracking(verify_has_metastore, verify_has_catalog, deployed_workflows)
    with pytest.raises(RuntimeWarning, match="Metastore not attached to workspace"):
        verify_progress_tracking.verify()
    verify_has_metastore.verify_metastore.assert_called_once()
    verify_has_catalog.assert_not_called()
    deployed_workflows.assert_not_called()


def test_verify_progress_tracking_raises_runtime_error_if_no_metastore(
    mock_installation
) -> None:
    verify_has_metastore = create_autospec(VerifyHasMetastore)
    verify_has_metastore.verify_metastore.return_value = False
    verify_has_catalog = create_autospec(VerifyHasCatalog)
    deployed_workflows = create_autospec(DeployedWorkflows)
    verify_progress_tracking = VerifyProgressTracking(verify_has_metastore, verify_has_catalog, deployed_workflows)
    with pytest.raises(RuntimeWarning, match="Metastore not attached to workspace"):
        verify_progress_tracking.verify()
    verify_has_metastore.verify_metastore.assert_called_once()
    verify_has_catalog.assert_not_called()
    deployed_workflows.assert_not_called()


def test_verify_progress_tracking_raises_runtime_error_if_missing_ucx_catalog(mock_installation) -> None:
    verify_has_metastore = create_autospec(VerifyHasMetastore)
    verify_has_catalog = create_autospec(VerifyHasCatalog)
    verify_has_catalog.verify.return_value = False
    deployed_workflows = create_autospec(DeployedWorkflows)
    verify_progress_tracking = VerifyProgressTracking(verify_has_metastore, verify_has_catalog, deployed_workflows)
    with pytest.raises(RuntimeWarning, match="UCX catalog not configured."):
        verify_progress_tracking.verify()
    verify_has_metastore.verify_metastore.assert_called_once()
    verify_has_catalog.verify.assert_called_once()
    deployed_workflows.assert_not_called()


def test_verify_progress_tracking_raises_runtime_error_if_assessment_workflow_did_not_run(mock_installation) -> None:
    ws = create_autospec(WorkspaceClient)
    verify_progress_tracking = VerifyProgressTracking(
        VerifyHasMetastore(ws),
        VerifyHasCatalog(ws, "ucx"),
        DeployedWorkflows(ws, InstallState.from_installation(mock_installation)),
    )
    with pytest.raises(RuntimeWarning, match="Assessment workflow did not complete successfully yet."):
        verify_progress_tracking.verify()

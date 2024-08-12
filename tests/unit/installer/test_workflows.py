from datetime import timedelta
from unittest.mock import create_autospec

import pytest
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.wheels import ProductInfo, WheelsV2
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, OperationFailed, ResourceDoesNotExist
from databricks.sdk.service.iam import ComplexValue, User
from databricks.sdk.service.jobs import BaseRun, CreateResponse, Run, RunOutput, RunResultState, RunState, RunTask

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.tasks import Task
from databricks.labs.ucx.installer.workflows import DeployedWorkflows, WorkflowsDeployment


class ResourceDoesNotExistIter:
    def __iter__(self):
        raise ResourceDoesNotExist("logs")


def test_deployed_workflows_handles_log_folder_does_not_exists(mock_installation):
    ws = create_autospec(WorkspaceClient)
    ws.jobs.list_runs.return_value = [BaseRun(run_id=456)]
    # Raise the error when the result is iterated over, NOT when the method is called.
    ws.workspace.list.return_value = ResourceDoesNotExistIter()
    install_state = InstallState.from_installation(mock_installation)
    deployed_workflows = DeployedWorkflows(ws, install_state, timedelta(minutes=2))

    deployed_workflows.relay_logs("test")

    ws.jobs.list_runs.assert_called_once_with(job_id="123", limit=1)


def side_effect_remove_after_in_tags_settings(**settings) -> CreateResponse:
    tags = settings.get("tags", {})
    _ = tags["RemoveAfter"]  # KeyError side effect
    return CreateResponse(job_id=1)


def test_workflows_deployment_creates_jobs_with_remove_after_tag(mock_installation):
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me.return_value = User(user_name="user", groups=[ComplexValue(display="admins")])
    ws.jobs.create.side_effect = side_effect_remove_after_in_tags_settings

    config = WorkspaceConfig("ucx")
    install_state = InstallState.from_installation(mock_installation)
    wheels = create_autospec(WheelsV2)
    product_info = ProductInfo.for_testing(WorkspaceConfig)
    tasks = [Task("workflow", "task", "docs", lambda *_: None)]
    workflows_deployment = WorkflowsDeployment(
        config,
        mock_installation,
        install_state,
        ws,
        wheels,
        product_info,
        verify_timeout=timedelta(minutes=5),
        tasks=tasks,
    )
    try:
        workflows_deployment.create_jobs()
    except KeyError as e:
        assert False, f"RemoveAfter tag not present: {e}"
    ws.current_user.me.assert_called_once()
    wheels.assert_not_called()


def test_run_workflow(mock_installation) -> None:
    """Check that run_workflow starts a workflow and waits for it to complete."""
    ws = create_autospec(WorkspaceClient)
    install_state = InstallState.from_installation(mock_installation)
    workflows = DeployedWorkflows(ws, install_state, verify_timeout=timedelta(seconds=0))
    ws.jobs.run_now.return_value = Run(run_id=456)
    ws.jobs.wait_get_run_job_terminated_or_skipped.return_value = Run(
        state=RunState(result_state=RunResultState.SUCCESS), start_time=0, end_time=1000, run_duration=1000
    )

    run_id = workflows.run_workflow("test")

    assert run_id == 456
    ws.jobs.run_now.assert_called_once_with(123)
    ws.jobs.wait_get_run_job_terminated_or_skipped.assert_called_once_with(run_id=456, timeout=timedelta(minutes=20))


def test_run_workflow_skip_job_wait(mock_installation) -> None:
    """Check that run_workflow can start a workflow but return immediately instead of waiting for it to complete."""
    ws = create_autospec(WorkspaceClient)
    install_state = InstallState.from_installation(mock_installation)
    workflows = DeployedWorkflows(ws, install_state, verify_timeout=timedelta(seconds=0))
    ws.jobs.run_now.return_value = Run(run_id=456)

    run_id = workflows.run_workflow("test", skip_job_wait=True)

    assert run_id == 456
    ws.jobs.run_now.assert_called_once_with(123)
    ws.jobs.wait_get_run_job_terminated_or_skipped.assert_not_called()


def test_run_workflow_operation_failed(mock_installation) -> None:
    """Check that run_workflow handles a failing workflow due to an OperationFailed error, including log replication."""
    ws = create_autospec(WorkspaceClient)
    install_state = InstallState.from_installation(mock_installation)
    workflows = DeployedWorkflows(ws, install_state, verify_timeout=timedelta(seconds=0))
    ws.jobs.run_now.return_value = Run(run_id=456)
    ws.jobs.wait_get_run_job_terminated_or_skipped.side_effect = OperationFailed("Simulated workflow failure")
    ws.jobs.get_run.return_value = Run(
        state=RunState(result_state=RunResultState.FAILED, state_message="Simulated failure"),
        tasks=[RunTask(run_id=789, task_key="test", state=RunState(result_state=RunResultState.FAILED))],
    )
    ws.jobs.get_run_output.return_value = RunOutput(error="Simulated [TABLE_OR_VIEW_NOT_FOUND] a_table")

    with pytest.raises(NotFound, match="a_table"):
        _ = workflows.run_workflow("test")

    ws.jobs.run_now.assert_called_once_with(123)
    ws.jobs.wait_get_run_job_terminated_or_skipped.assert_called_once_with(run_id=456, timeout=timedelta(minutes=20))
    ws.jobs.get_run.assert_called_once_with(456)
    ws.workspace.list.assert_called_once_with("~/mock/logs/test")
    ws.jobs.get_run_output.assert_called_once_with(789)


def test_run_workflow_timeout(mock_installation) -> None:
    """Check that run_workflow can handle a workflow takes longer than the timeout, including log replication."""
    ws = create_autospec(WorkspaceClient)
    install_state = InstallState.from_installation(mock_installation)
    workflows = DeployedWorkflows(ws, install_state, verify_timeout=timedelta(seconds=0))
    ws.jobs.run_now.return_value = Run(run_id=456)
    ws.jobs.wait_get_run_job_terminated_or_skipped.side_effect = TimeoutError("Simulated timeout")

    with pytest.raises(TimeoutError):
        _ = workflows.run_workflow("test", max_wait=timedelta(minutes=2))

    ws.jobs.run_now.assert_called_once_with(123)
    ws.jobs.wait_get_run_job_terminated_or_skipped.assert_called_once_with(run_id=456, timeout=timedelta(minutes=2))
    ws.workspace.list.assert_called_once_with("~/mock/logs/test")

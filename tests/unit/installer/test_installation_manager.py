import io
from unittest.mock import MagicMock

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.service.iam import ComplexValue, User
from databricks.sdk.service.jobs import (
    BaseRun,
    RunLifeCycleState,
    RunResultState,
    RunState,
)

from databricks.labs.ucx.framework.parallel import ManyError
from databricks.labs.ucx.install import WorkspaceInstaller
from databricks.labs.ucx.installer import InstallationManager


def test_happy_path(mocker):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")

    ws.users.list.return_value = [User(user_name="foo")]
    ws.workspace.download.return_value = io.StringIO("version: 2\ninventory_database: bar")

    installation_manager = InstallationManager(ws)
    user_installations = installation_manager.user_installations()
    assert len(user_installations) == 1

    assert user_installations[0].user.user_name == "foo"
    assert user_installations[0].config.inventory_database == "bar"


def test_config_not_found(mocker):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")

    ws.users.list.return_value = [User(user_name="foo")]
    ws.workspace.download.side_effect = NotFound(...)

    installation_manager = InstallationManager(ws)
    user_installations = installation_manager.user_installations()
    assert len(user_installations) == 0


def test_other_strange_errors(mocker):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")

    ws.users.list.return_value = [User(user_name="foo")]
    ws.workspace.download.side_effect = ValueError("nope")

    installation_manager = InstallationManager(ws)
    with pytest.raises(ManyError):
        installation_manager.user_installations()


def test_corrupt_config(mocker):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")

    ws.users.list.return_value = [User(user_name="foo")]
    ws.workspace.download.return_value = io.StringIO("version: 2\ntacl: true")

    installation_manager = InstallationManager(ws)
    user_installations = installation_manager.user_installations()
    assert len(user_installations) == 0


def test_validate_assessment(mocker):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")
    current_user = MagicMock()
    current_user.me.return_value = User(user_name="foo", groups=[ComplexValue(display="admins")])

    state = MagicMock()
    state.jobs = {"assessment": 123}

    ws.current_user = current_user
    ws.jobs.list_runs.return_value = [
        BaseRun(run_id=123, state=RunState(result_state=RunResultState.SUCCESS)),
        BaseRun(run_id=111, state=RunState(result_state=RunResultState.FAILED)),
    ]
    ws.jobs.wait_get_run_job_terminated_or_skipped = MagicMock(return_value=None)
    installation_manager = WorkspaceInstaller(ws)
    installation_manager._state = state

    assert installation_manager.validate_step("assessment")

    ws.jobs.list_runs.return_value = [
        BaseRun(run_id=123, state=RunState(result_state=RunResultState.FAILED)),
        BaseRun(run_id=111, state=RunState(result_state=RunResultState.FAILED)),
    ]

    assert not installation_manager.validate_step("assessment")

    ws.jobs.list_runs.return_value = [
        BaseRun(run_id=123, state=RunState(result_state=RunResultState.FAILED)),
        BaseRun(run_id=111, state=RunState(life_cycle_state=RunLifeCycleState.RUNNING)),
    ]

    installation_manager.validate_step("assessment")
    ws.jobs.wait_get_run_job_terminated_or_skipped.assert_called()


def test_validate_run_assessment(mocker):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")
    current_user = MagicMock()
    current_user.me.return_value = User(user_name="foo", groups=[ComplexValue(display="admins")])

    state = MagicMock()
    state.jobs = {"assessment": 123}

    ws.current_user = current_user
    installation_manager = WorkspaceInstaller(ws)
    installation_manager._state = state
    installation_manager.validate_step = MagicMock(return_value=True)
    # Test a use case where assessment ran successfully
    installation_manager.validate_and_run("assessment")
    installation_manager.validate_step.assert_called_with("assessment")

    # Test a use case where assessment didn't run successfully
    installation_manager.run_workflow = MagicMock()
    installation_manager.validate_step = MagicMock(return_value=False)
    installation_manager.validate_and_run("assessment")
    installation_manager.validate_step.assert_called_with("assessment")
    installation_manager.run_workflow.assert_called_with("assessment")

from datetime import timedelta
from unittest.mock import create_autospec

from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.wheels import ProductInfo, WheelsV2
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import ComplexValue, User
from databricks.sdk.service.jobs import CreateResponse

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.tasks import Task
from databricks.labs.ucx.installer.workflows import WorkflowsDeployment


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

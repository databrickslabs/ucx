import datetime as dt
import pytest

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from databricks.labs.ucx.progress.workflow_runs import WorkflowRunRecorder


class MockWorkspaceClient:
    """Mock workspace client"""

    def get_workspace_id(self) -> int:
        return 123456789


@pytest.mark.parametrize(
    "attribute, klass",
    [
        ("workflow_run_recorder", WorkflowRunRecorder),
    ],
)
def test_context_attribute_class(tmp_path, attribute, klass) -> None:
    """Increase coverage with these tests"""
    config = WorkspaceConfig("str")
    named_parameters = {
        "workflow": "test",
        "job_id": "123",
        "parent_run_id": "456",
        "start_time": dt.datetime.now(tz=dt.timezone.utc).replace(microsecond=0).isoformat(),
    }

    ctx = RuntimeContext(named_parameters=named_parameters).replace(
        config=config, sql_backend=None, workspace_client=MockWorkspaceClient()
    )

    assert hasattr(ctx, attribute)
    assert isinstance(getattr(ctx, attribute), klass)

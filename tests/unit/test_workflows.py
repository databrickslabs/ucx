from databricks.labs.ucx.runtime import Workflows


def test_workflows_detected() -> None:
    workflows = Workflows.all()

    assert len(workflows.workflows()) > 0, "No workflows detected"


def test_tasks_detected() -> None:
    workflows = Workflows.all()

    for name, workflow in workflows.workflows().items():
        tasks = list(workflow.tasks())
        assert len(tasks) > 0, f"No tasks detected for workflow: {name}"

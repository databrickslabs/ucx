from databricks.labs.ucx.runtime import Workflows


def test_workflows_detected() -> None:
    all_workflows = Workflows.all()

    assert len(all_workflows.workflows) > 0, "No workflows detected"


def test_tasks_detected() -> None:
    all_workflows = Workflows.all()

    for name, workflow in all_workflows.workflows.items():
        tasks = list(workflow.tasks())
        assert len(tasks) > 0, f"No tasks detected for workflow: {name}"

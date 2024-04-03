from databricks.labs.ucx.runtime_v2 import Workflows


def test_tasks_detected():
    workflows = Workflows.all()

    tasks = workflows.tasks()

    assert len(tasks) > 1

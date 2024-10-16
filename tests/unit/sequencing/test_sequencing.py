from unittest.mock import create_autospec

from databricks.sdk.service import iam, jobs
from pathlib import Path

from databricks.sdk.service import jobs
from databricks.sdk.service.compute import ClusterDetails
from databricks.sdk.service.jobs import NotebookTask

from databricks.labs.ucx.framework.owners import AdministratorLocator, AdministratorFinder
from databricks.labs.ucx.sequencing.sequencing import MigrationSequencer
from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.graph import DependencyGraph
from databricks.labs.ucx.source_code.jobs import WorkflowTask


def test_sequencer_builds_cluster_and_children_from_task(ws, simple_dependency_resolver, mock_path_lookup):
    ws.clusters.get.return_value = ClusterDetails(cluster_name="my-cluster", creator_user_name="John Doe")
    task = jobs.Task(task_key="test-task", existing_cluster_id="cluster-123")
    settings = jobs.JobSettings(name="test-job", tasks=[task])
    job = jobs.Job(job_id=1234, settings=settings)
    ws.jobs.get.return_value = job
    dependency = WorkflowTask(ws, task, job)
    graph = DependencyGraph(dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState())
    admin_finder = create_autospec(AdministratorFinder)
    admin_user = iam.User(user_name="John Doe", active=True, roles=[iam.ComplexValue(value="account_admin")])
    admin_finder.find_admin_users.return_value = (admin_user,)
    sequencer = MigrationSequencer(ws, AdministratorLocator(ws, finders=[lambda _ws: admin_finder]))
    sequencer.register_workflow_task(task, job, graph)
    steps = list(sequencer.generate_steps())
    step = steps[-1]
    assert step.step_id
    assert step.object_type == "CLUSTER"
    assert step.object_id == "cluster-123"
    assert step.object_name == "my-cluster"
    assert step.object_owner == "John Doe"
    assert step.step_number == 3
    assert len(step.required_step_ids) == 2


def test_sequencer_builds_steps_from_dependency_graph(ws, simple_dependency_resolver, mock_path_lookup):
    functional = mock_path_lookup.resolve(Path("functional"))
    mock_path_lookup.append_path(functional)
    mock_path_lookup = mock_path_lookup.change_directory(functional)
    notebook_path = Path("grand_parent_that_imports_parent_that_magic_runs_child.py")
    notebook_task = NotebookTask(notebook_path=notebook_path.as_posix())
    task = jobs.Task(task_key="test-task", existing_cluster_id="cluster-123", notebook_task=notebook_task)
    settings = jobs.JobSettings(name="test-job", tasks=[task])
    job = jobs.Job(job_id=1234, settings=settings)
    ws.jobs.get.return_value = job
    dependency = WorkflowTask(ws, task, job)
    container = dependency.load(mock_path_lookup)
    graph = DependencyGraph(dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState())
    problems = container.build_dependency_graph(graph)
    assert not problems
    sequencer = MigrationSequencer(ws, mock_path_lookup)
    sequencer.register_workflow_task(task, job, graph)
    steps = list(sequencer.generate_steps())
    names = {step.object_name for step in steps}
    assert notebook_path.as_posix() in names
    notebook_path = Path("parent_that_magic_runs_child_that_uses_value_from_parent.py")
    assert notebook_path.as_posix() in names
    notebook_path = Path("_child_that_uses_value_from_parent.py")
    assert notebook_path.as_posix() in names

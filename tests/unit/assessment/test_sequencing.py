import dataclasses
from unittest.mock import create_autospec

from databricks.sdk.service import iam, jobs
from databricks.sdk.service.compute import ClusterDetails

from databricks.labs.ucx.assessment.sequencing import MigrationSequencer, MigrationStep
from databricks.labs.ucx.framework.owners import AdministratorLocator, AdministratorFinder
from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.graph import DependencyGraph
from databricks.labs.ucx.source_code.jobs import WorkflowTask


def test_cluster_from_task_has_children(ws, simple_dependency_resolver, mock_path_lookup):
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

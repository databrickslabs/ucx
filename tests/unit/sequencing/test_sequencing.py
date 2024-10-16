from databricks.sdk.service import jobs
from databricks.sdk.service.compute import ClusterDetails

from databricks.labs.ucx.sequencing.sequencing import MigrationSequencer
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
    sequencer = MigrationSequencer(ws)
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

from unittest.mock import create_autospec

from databricks.sdk.service import iam, jobs
from databricks.sdk.service.compute import ClusterDetails

from databricks.labs.ucx.assessment.sequencing import MigrationSequencer, MigrationStep
from databricks.labs.ucx.framework.owners import AdministratorLocator, AdministratorFinder
from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.graph import DependencyGraph
from databricks.labs.ucx.source_code.jobs import WorkflowTask


def test_sequence_steps_from_job_task_with_cluster(ws, simple_dependency_resolver, mock_path_lookup) -> None:
    """Sequence a job with a task referencing a cluster.

    Sequence:  # TODO: @JCZuurmond: Would expect cluster first.
    1. Task
    2. Job
    3. Cluster
    """
    task = jobs.Task(task_key="test-task", existing_cluster_id="cluster-123")
    settings = jobs.JobSettings(name="test-job", tasks=[task])
    job = jobs.Job(job_id=1234, settings=settings)
    ws.jobs.get.return_value = job

    # Match task cluster above on cluster id
    def get_cluster(cluster_id: str) -> ClusterDetails:
        if cluster_id == "cluster-123":
            return ClusterDetails(cluster_id="cluster-123", cluster_name="my-cluster", creator_user_name="John Doe")
        raise ValueError(f"Unknown cluster: {cluster_id}")

    ws.clusters.get.side_effect = get_cluster

    # Match cluster creator above on username
    admin_finder = create_autospec(AdministratorFinder)
    admin_user = iam.User(user_name="John Doe", active=True, roles=[iam.ComplexValue(value="account_admin")])
    admin_finder.find_admin_users.return_value = (admin_user,)

    dependency = WorkflowTask(ws, task, job)
    graph = DependencyGraph(dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState())
    sequencer = MigrationSequencer(ws, AdministratorLocator(ws, finders=[lambda _ws: admin_finder]))
    sequencer.register_workflow_task(task, job, graph)

    steps = list(sequencer.generate_steps())

    assert steps == [
        MigrationStep(
            step_id=2,
            step_number=1,
            object_type="TASK",
            object_id="1234/test-task",
            object_name="test-task",
            object_owner="John Doe",
            required_step_ids=[],
        ),
        MigrationStep(
            step_id=1,
            step_number=2,
            object_type="JOB",
            object_id="1234",
            object_name="test-job",
            object_owner="John Doe",
            required_step_ids=[2],
        ),
        MigrationStep(
            step_id=3,
            step_number=3,
            object_type="CLUSTER",
            object_id="cluster-123",
            object_name="my-cluster",
            object_owner="John Doe",
            required_step_ids=[1, 2],
        ),
    ]

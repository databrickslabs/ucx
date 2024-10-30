from unittest.mock import create_autospec

import pytest
from databricks.sdk.errors import ResourceDoesNotExist
from databricks.sdk.service import iam, jobs
from databricks.sdk.service.compute import ClusterDetails, ClusterSpec

from databricks.labs.ucx.assessment.sequencing import MigrationSequencer, MigrationStep
from databricks.labs.ucx.framework.owners import AdministratorLocator, AdministratorFinder
from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.graph import DependencyGraph
from databricks.labs.ucx.source_code.jobs import WorkflowTask


@pytest.fixture
def admin_locator(ws):
    """Create a mock for an `class:AdminLocator`"""
    admin_finder = create_autospec(AdministratorFinder)
    admin_user = iam.User(user_name="John Doe", active=True, roles=[iam.ComplexValue(value="account_admin")])
    admin_finder.find_admin_users.return_value = (admin_user,)
    return AdministratorLocator(ws, finders=[lambda _ws: admin_finder])


def test_register_job_with_existing_cluster(ws, admin_locator) -> None:
    """Register a existing cluster."""
    task = jobs.Task(task_key="test-task", existing_cluster_id="cluster-123")
    settings = jobs.JobSettings(name="test-job", tasks=[task])
    job = jobs.Job(job_id=1234, settings=settings)

    def get_cluster(cluster_id: str) -> ClusterDetails:
        if cluster_id == "cluster-123":
            return ClusterDetails(cluster_id="cluster-123", cluster_name="my-cluster")
        raise ResourceDoesNotExist(f"Unknown cluster: {cluster_id}")

    ws.clusters.get.side_effect = get_cluster
    sequencer = MigrationSequencer(ws, admin_locator)

    maybe_node = sequencer.register_job(job)

    assert not maybe_node.failed


def test_register_non_existing_cluster(ws, admin_locator) -> None:
    """Register a non existing cluster."""
    ws.clusters.get.side_effect = ResourceDoesNotExist("Unknown cluster")
    sequencer = MigrationSequencer(ws, admin_locator)

    maybe_node = sequencer._register_cluster("non-existing-id")

    assert maybe_node.node is None
    assert maybe_node.failed
    assert maybe_node.problems == ["Could not find cluster: non-existing-id"]


def test_register_non_existing_job_cluster(
    ws,
    mock_path_lookup,
    admin_locator,
) -> None:
    """Register a non-existing job cluster."""
    job_cluster = jobs.JobCluster(new_cluster=ClusterSpec(), job_cluster_key="non-existing-id")
    sequencer = MigrationSequencer(ws, admin_locator)

    maybe_node = sequencer._register_job_cluster(job_cluster)

    assert maybe_node.node is None
    assert maybe_node.failed
    assert maybe_node.problems == ["Could not find cluster: non-existing-id"]


def test_register_workflow_task_with_missing_cluster_dependency(
    ws,
    simple_dependency_resolver,
    mock_path_lookup,
    admin_locator,
) -> None:
    """Register a workflow task with missing cluster dependency."""
    task = jobs.Task(task_key="test-task", existing_cluster_id="cluster-123")
    settings = jobs.JobSettings(name="test-job", tasks=[task])
    job = jobs.Job(job_id=1234, settings=settings)
    ws.jobs.get.return_value = job

    ws.clusters.get.side_effect = ResourceDoesNotExist("Unknown cluster")

    dependency = WorkflowTask(ws, task, job)
    graph = DependencyGraph(dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState())
    sequencer = MigrationSequencer(ws, admin_locator)

    maybe_node = sequencer._register_workflow_task(task, job, graph)

    assert maybe_node.node is None
    assert maybe_node.failed()


def test_sequence_steps_from_job_task_with_cluster(
    ws, simple_dependency_resolver, mock_path_lookup, admin_locator
) -> None:
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
    admin_user = admin_locator.get_workspace_administrator()

    def get_cluster(cluster_id: str) -> ClusterDetails:
        if cluster_id == "cluster-123":
            return ClusterDetails(cluster_id="cluster-123", cluster_name="my-cluster", creator_user_name=admin_user)
        raise ResourceDoesNotExist(f"Unknown cluster: {cluster_id}")

    ws.clusters.get.side_effect = get_cluster

    dependency = WorkflowTask(ws, task, job)
    graph = DependencyGraph(dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState())
    sequencer = MigrationSequencer(ws, admin_locator)
    sequencer._register_workflow_task(task, job, graph)

    steps = list(sequencer.generate_steps())

    assert steps == [
        MigrationStep(
            step_id=2,
            step_number=0,
            object_type="TASK",
            object_id="1234/test-task",
            object_name="test-task",
            object_owner="John Doe",
            required_step_ids=[],
        ),
        MigrationStep(
            step_id=1,
            step_number=1,
            object_type="JOB",
            object_id="1234",
            object_name="test-job",
            object_owner="John Doe",
            required_step_ids=[2],
        ),
        MigrationStep(
            step_id=3,
            step_number=2,
            object_type="CLUSTER",
            object_id="cluster-123",
            object_name="my-cluster",
            object_owner="John Doe",
            required_step_ids=[1, 2],
        ),
    ]

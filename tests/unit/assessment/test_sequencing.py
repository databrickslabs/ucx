from unittest.mock import create_autospec

import pytest
from databricks.sdk.errors import ResourceDoesNotExist
from databricks.sdk.service import iam, jobs
from databricks.sdk.service.compute import ClusterDetails, ClusterSpec

from databricks.labs.ucx.assessment.sequencing import MigrationSequencer, MigrationStep
from databricks.labs.ucx.framework.owners import AdministratorLocator, AdministratorFinder
from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.graph import DependencyGraph, DependencyProblem
from databricks.labs.ucx.source_code.jobs import WorkflowTask


@pytest.fixture
def admin_locator(ws):
    """Create a mock for an `class:AdminLocator`"""
    admin_finder = create_autospec(AdministratorFinder)
    admin_user = iam.User(user_name="John Doe", active=True, roles=[iam.ComplexValue(value="account_admin")])
    admin_finder.find_admin_users.return_value = (admin_user,)
    return AdministratorLocator(ws, finders=[lambda _ws: admin_finder])


def test_register_job_with_existing_cluster(ws, admin_locator) -> None:
    """Register an existing cluster."""
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


def test_register_job_with_non_existing_cluster(ws, admin_locator) -> None:
    """Register a non existing cluster."""
    task = jobs.Task(task_key="test-task", existing_cluster_id="non-existing-id")
    settings = jobs.JobSettings(name="test-job", tasks=[task])
    job = jobs.Job(job_id=1234, settings=settings)

    ws.clusters.get.side_effect = ResourceDoesNotExist("Unknown cluster")
    sequencer = MigrationSequencer(ws, admin_locator)

    maybe_node = sequencer.register_job(job)

    assert maybe_node.failed
    assert maybe_node.problems == [
        DependencyProblem(
            code="cluster-not-found",
            message="Could not find cluster: non-existing-id",
        )
    ]


def test_register_job_with_existing_job_cluster_key(
    ws,
    mock_path_lookup,
    admin_locator,
) -> None:
    """Register a job with existing job cluster key."""
    job_cluster = jobs.JobCluster("existing-id", ClusterSpec())
    task = jobs.Task(task_key="test-task", job_cluster_key="existing-id")
    settings = jobs.JobSettings(name="test-job", tasks=[task], job_clusters=[job_cluster])
    job = jobs.Job(job_id=1234, settings=settings)
    sequencer = MigrationSequencer(ws, admin_locator)

    maybe_node = sequencer.register_job(job)

    assert not maybe_node.failed


def test_register_job_with_non_existing_job_cluster_key(
    ws,
    mock_path_lookup,
    admin_locator,
) -> None:
    """Register a job with non-existing job cluster key."""
    task = jobs.Task(task_key="test-task", job_cluster_key="non-existing-id")
    settings = jobs.JobSettings(name="test-job", tasks=[task])
    job = jobs.Job(job_id=1234, settings=settings)

    sequencer = MigrationSequencer(ws, admin_locator)

    maybe_node = sequencer.register_job(job)

    assert maybe_node.failed
    assert maybe_node.problems == [
        DependencyProblem(
            code="cluster-not-found",
            message="Could not find cluster: non-existing-id",
        )
    ]


def test_register_job_with_new_cluster(
    ws,
    simple_dependency_resolver,
    mock_path_lookup,
    admin_locator,
) -> None:
    """Register a workflow task with a new cluster."""
    task = jobs.Task(task_key="test-task", new_cluster=ClusterSpec())
    settings = jobs.JobSettings(name="test-job", tasks=[task])
    job = jobs.Job(job_id=1234, settings=settings)
    ws.jobs.get.return_value = job
    sequencer = MigrationSequencer(ws, admin_locator)

    maybe_node = sequencer.register_job(job)

    assert not maybe_node.failed


def test_sequence_steps_from_job_task_with_existing_cluster_id(
    ws, simple_dependency_resolver, mock_path_lookup, admin_locator
) -> None:
    """Sequence a job with a task referencing an existing cluster.

    Sequence:  # TODO: @JCZuurmond: Would expect cluster first.
    1. Task
    2. Job
    3. Cluster
    """
    task = jobs.Task(task_key="test-task", existing_cluster_id="cluster-123")
    settings = jobs.JobSettings(name="test-job", tasks=[task])
    job = jobs.Job(job_id=1234, settings=settings)

    # Match task cluster above on cluster id
    admin_user = admin_locator.get_workspace_administrator()

    def get_cluster(cluster_id: str) -> ClusterDetails:
        if cluster_id == "cluster-123":
            return ClusterDetails(cluster_id="cluster-123", cluster_name="my-cluster", creator_user_name=admin_user)
        raise ResourceDoesNotExist(f"Unknown cluster: {cluster_id}")

    ws.clusters.get.side_effect = get_cluster

    sequencer = MigrationSequencer(ws, admin_locator)
    sequencer.register_job(job)

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

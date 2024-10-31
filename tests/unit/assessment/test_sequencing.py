from unittest.mock import create_autospec

import pytest
from databricks.sdk.errors import ResourceDoesNotExist
from databricks.sdk.service import iam, jobs
from databricks.sdk.service.compute import ClusterDetails, ClusterSpec

from databricks.labs.ucx.assessment.sequencing import MigrationSequencer, MigrationStep
from databricks.labs.ucx.framework.owners import AdministratorLocator, AdministratorFinder
from databricks.labs.ucx.source_code.graph import DependencyProblem


@pytest.fixture
def admin_locator(ws):
    """Create a mock for an `class:AdminLocator`"""
    admin_finder = create_autospec(AdministratorFinder)
    admin_user = iam.User(user_name="John Doe", active=True, roles=[iam.ComplexValue(value="account_admin")])
    admin_finder.find_admin_users.return_value = (admin_user,)
    return AdministratorLocator(ws, finders=[lambda _ws: admin_finder])


def test_register_job_with_existing_cluster(ws, admin_locator) -> None:
    """Register a job with a task referencing an existing cluster."""
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
    """Register a job with a task referencing a non-existing cluster."""
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


def test_register_job_with_existing_job_cluster_key(ws, admin_locator) -> None:
    """Register a job with a task referencing a existing job cluster."""
    job_cluster = jobs.JobCluster("existing-id", ClusterSpec())
    task = jobs.Task(task_key="test-task", job_cluster_key="existing-id")
    settings = jobs.JobSettings(name="test-job", tasks=[task], job_clusters=[job_cluster])
    job = jobs.Job(job_id=1234, settings=settings)
    sequencer = MigrationSequencer(ws, admin_locator)

    maybe_node = sequencer.register_job(job)

    assert not maybe_node.failed


def test_register_job_with_non_existing_job_cluster_key(ws, admin_locator) -> None:
    """Register a job with a task referencing a non-existing job cluster."""
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


def test_register_job_with_new_cluster(ws, admin_locator) -> None:
    """Register a job with a task with a new cluster definition."""
    task = jobs.Task(task_key="test-task", new_cluster=ClusterSpec())
    settings = jobs.JobSettings(name="test-job", tasks=[task])
    job = jobs.Job(job_id=1234, settings=settings)
    ws.jobs.get.return_value = job
    sequencer = MigrationSequencer(ws, admin_locator)

    maybe_node = sequencer.register_job(job)

    assert not maybe_node.failed


def test_sequence_steps_from_job_task_with_existing_cluster_id(ws, admin_locator) -> None:
    """Sequence a job with a task referencing an existing cluster.

    Sequence:
    1. Cluster
    2. Task
    3. Job
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
            object_type="CLUSTER",
            object_id="cluster-123",
            object_name="my-cluster",
            object_owner="John Doe",
            required_step_ids=[],
        ),
        MigrationStep(
            step_id=1,
            step_number=1,
            object_type="TASK",
            object_id="1234/test-task",
            object_name="test-task",
            object_owner="John Doe",
            required_step_ids=[2],
        ),
        MigrationStep(
            step_id=0,
            step_number=2,
            object_type="JOB",
            object_id="1234",
            object_name="test-job",
            object_owner="John Doe",
            required_step_ids=[1],
        ),
    ]


def test_sequence_steps_from_job_task_with_existing_job_cluster_key(ws, admin_locator) -> None:
    """Sequence a job with a task referencing an existing job cluster.

    Sequence:
    1. Job cluster
    2. Task
    3. Job
    """
    job_cluster = jobs.JobCluster("existing-id", ClusterSpec())
    task = jobs.Task(task_key="test-task", job_cluster_key="existing-id")
    settings = jobs.JobSettings(name="test-job", tasks=[task], job_clusters=[job_cluster])
    job = jobs.Job(job_id=1234, settings=settings)
    sequencer = MigrationSequencer(ws, admin_locator)
    sequencer.register_job(job)

    steps = list(sequencer.generate_steps())

    assert steps == [
        MigrationStep(
            step_id=1,
            step_number=0,
            object_type="CLUSTER",
            object_id="1234/existing-id",
            object_name="existing-id",
            object_owner="John Doe",
            required_step_ids=[],
        ),
        MigrationStep(
            step_id=2,
            step_number=1,
            object_type="TASK",
            object_id="1234/test-task",
            object_name="test-task",
            object_owner="John Doe",
            required_step_ids=[1],
        ),
        MigrationStep(
            step_id=0,
            step_number=2,
            object_type="JOB",
            object_id="1234",
            object_name="test-job",
            object_owner="John Doe",
            required_step_ids=[1, 2],
        ),
    ]


def test_sequence_steps_from_job_task_with_new_cluster(ws, admin_locator) -> None:
    """Sequence a job with a task that has a new cluster definition.

    Sequence:
    1. Task  # The cluster is part of the task, not a separate step in the sequence
    2. Job
    """
    task = jobs.Task(task_key="test-task", new_cluster=ClusterSpec())
    settings = jobs.JobSettings(name="test-job", tasks=[task])
    job = jobs.Job(job_id=1234, settings=settings)
    sequencer = MigrationSequencer(ws, admin_locator)
    sequencer.register_job(job)

    steps = list(sequencer.generate_steps())

    assert steps == [
        MigrationStep(
            step_id=1,
            step_number=0,
            object_type="TASK",
            object_id="1234/test-task",
            object_name="test-task",
            object_owner="John Doe",
            required_step_ids=[],
        ),
        MigrationStep(
            step_id=0,
            step_number=1,
            object_type="JOB",
            object_id="1234",
            object_name="test-job",
            object_owner="John Doe",
            required_step_ids=[1],
        ),
    ]


def test_sequence_steps_from_job_task_with_non_existing_cluster(ws, admin_locator) -> None:
    """Sequence a job with a task that references a non-existing cluster.

    Sequence:
    1. Cluster  # TODO: Do we still expect this reference?
    2. Task
    3. Job
    """
    ws.clusters.get.side_effect = ResourceDoesNotExist("Unknown cluster")
    task = jobs.Task(task_key="test-task", existing_cluster_id="non-existing-id")
    settings = jobs.JobSettings(name="test-job", tasks=[task])
    job = jobs.Job(job_id=1234, settings=settings)
    sequencer = MigrationSequencer(ws, admin_locator)
    sequencer.register_job(job)

    steps = list(sequencer.generate_steps())

    assert steps == [
        MigrationStep(
            step_id=2,
            step_number=0,
            object_type="CLUSTER",
            object_id="non-existing-id",
            object_name="non-existing-id",
            object_owner="John Doe",
            required_step_ids=[],
        ),
        MigrationStep(
            step_id=1,
            step_number=0,
            object_type="TASK",
            object_id="1234/test-task",
            object_name="test-task",
            object_owner="John Doe",
            required_step_ids=[2],
        ),
        MigrationStep(
            step_id=0,
            step_number=1,
            object_type="JOB",
            object_id="1234",
            object_name="test-job",
            object_owner="John Doe",
            required_step_ids=[1],
        ),
    ]


def test_sequence_steps_from_job_task_referencing_other_task(ws, admin_locator) -> None:
    """Sequence a job with a task that has a new cluster definition.

    Sequence:
    1. Task1
    2. Task2
    3. Job
    """
    task1 = jobs.Task(task_key="task1")
    task_dependency = jobs.TaskDependency(task1.task_key)
    task2 = jobs.Task(task_key="task2", depends_on=[task_dependency])
    tasks = [task2, task1]  # Reverse order on purpose to test if this is handled
    settings = jobs.JobSettings(name="job", tasks=tasks)
    job = jobs.Job(job_id=1234, settings=settings)
    sequencer = MigrationSequencer(ws, admin_locator)

    maybe_job_node = sequencer.register_job(job)
    assert not maybe_job_node.failed

    steps = list(sequencer.generate_steps())
    assert steps == [
        MigrationStep(
            step_id=2,
            step_number=0,
            object_type="TASK",
            object_id="1234/task1",
            object_name="task1",
            object_owner="John Doe",
            required_step_ids=[],
        ),
        MigrationStep(
            step_id=1,
            step_number=1,
            object_type="TASK",
            object_id="1234/task2",
            object_name="task2",
            object_owner="John Doe",
            required_step_ids=[2],
        ),
        MigrationStep(
            step_id=0,
            step_number=2,
            object_type="JOB",
            object_id="1234",
            object_name="job",
            object_owner="John Doe",
            required_step_ids=[1, 2],
        ),
    ]

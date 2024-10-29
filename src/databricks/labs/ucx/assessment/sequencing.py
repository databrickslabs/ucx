from __future__ import annotations

import heapq
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TypeVar

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

from databricks.labs.ucx.assessment.clusters import ClusterOwnership, ClusterInfo
from databricks.labs.ucx.assessment.jobs import JobOwnership, JobInfo
from databricks.labs.ucx.framework.owners import AdministratorLocator
from databricks.labs.ucx.source_code.graph import DependencyGraph


@dataclass
class MigrationStep:
    step_id: int
    step_number: int
    object_type: str
    object_id: str
    object_name: str
    object_owner: str
    required_step_ids: list[int]


MigrationNodeKey = tuple[str, str]


@dataclass
class MigrationNode:
    # TODO: @JCZuurmond the prefixes look a bit redundant
    node_id: int = field(compare=False)
    """Globally unique id."""

    object_type: str
    """Object type. Together with `attr:object_id` a unique identifier."""

    object_id: str
    """Object id. Together with `attr:object_id` a unique identifier."""

    object_name: str = field(compare=False)
    """Object name, more human friendly than `attr:object_id`."""

    object_owner: str = field(compare=False)
    """Object owner."""

    @property
    def key(self) -> MigrationNodeKey:
        """Unique identifier of the node."""
        return self.object_type, self.object_id

    def as_step(self, step_number: int, required_step_ids: list[int]) -> MigrationStep:
        """Convert to class:MigrationStep."""
        return MigrationStep(
            step_id=self.node_id,
            step_number=step_number,
            object_type=self.object_type,
            object_id=self.object_id,
            object_name=self.object_name,
            object_owner=self.object_owner,
            required_step_ids=required_step_ids,
        )


QueueTask = TypeVar("QueueTask")
QueueEntry = list[int, int, QueueTask | str]


class PriorityQueue:
    """A priority queue supporting to update tasks.

    An adaption from class:queue.Priority to support updating tasks.

    Note:
        This implementation does not support threading safety as that is not required.

    Source:
        See https://docs.python.org/3/library/heapq.html#priority-queue-implementation-notes on the changes below
        to handle priority changes in the task.
    """

    _REMOVED = "<removed>"  # Mark removed items
    _UPDATED = "<updated>"  # Mark updated items

    def __init__(self):
        self._entries: list[QueueEntry] = []
        self._entry_finder: dict[QueueTask, QueueEntry] = {}
        self._counter = 0  # Tiebreaker with equal priorities, then "first in, first out"

    def put(self, priority: int, task: QueueTask) -> None:
        """Put or update task in the queue.

        The lowest priority is retrieved from the queue first.
        """
        if task in self._entry_finder:
            raise KeyError(f"Use `:meth:update` to update existing task: {task}")
        entry = [priority, self._counter, task]
        self._entry_finder[task] = entry
        heapq.heappush(self._entries, entry)
        self._counter += 1

    def get(self) -> QueueTask | None:
        """Gets the tasks with lowest priority."""
        while self._entries:
            _, _, task = heapq.heappop(self._entries)
            if task in (self._REMOVED, self._UPDATED):
                continue
            self._remove(task)
            # Ignore type because heappop returns Any, while we know it is an QueueEntry
            return task  # type: ignore
        return None

    def _remove(self, task: QueueTask) -> None:
        """Remove a task from the queue."""
        entry = self._entry_finder.pop(task)
        entry[2] = self._REMOVED

    def update(self, priority: int, task: QueueTask) -> None:
        """Update a task in the queue."""
        entry = self._entry_finder.pop(task)
        if entry is None:
            raise KeyError(f"Cannot update unknown task: {task}")
        if entry[2] != self._REMOVED:  # Do not update REMOVED tasks
            entry[2] = self._UPDATED
            self.put(priority, task)


class MigrationSequencer:
    """Sequence the migration dependencies in order to execute the migration.

    Similar to the other graph logic, we first build the graph by registering dependencies, then we analyse the graph.
    Analysing the graph in this case means: computing the migration sequence in `meth:generate_steps`.
    """

    def __init__(self, ws: WorkspaceClient, admin_locator: AdministratorLocator):
        self._ws = ws
        self._admin_locator = admin_locator
        self._last_node_id = 0
        self._nodes: dict[MigrationNodeKey, MigrationNode] = {}
        self._outgoing: dict[MigrationNodeKey, set[MigrationNodeKey]] = defaultdict(set)

    def register_workflow_task(self, task: jobs.Task, job: jobs.Job, _graph: DependencyGraph) -> MigrationNode:
        task_id = f"{job.job_id}/{task.task_key}"
        task_node = self._nodes.get(("TASK", task_id), None)
        if task_node:
            return task_node
        job_node = self.register_workflow_job(job)
        self._last_node_id += 1
        task_node = MigrationNode(
            node_id=self._last_node_id,
            object_type="TASK",
            object_id=task_id,
            object_name=task.task_key,
            object_owner=job_node.object_owner,  # no task owner so use job one
        )
        self._nodes[task_node.key] = task_node
        self._outgoing[task_node.key].add(job_node.key)
        if task.existing_cluster_id:
            cluster_node = self.register_cluster(task.existing_cluster_id)
            if cluster_node:
                self._outgoing[task_node.key].add(cluster_node.key)
                # also make the cluster dependent on the job
                self._outgoing[job_node.key].add(cluster_node.key)
        # TODO register dependency graph
        return task_node

    def register_workflow_job(self, job: jobs.Job) -> MigrationNode:
        job_node = self._nodes.get(("JOB", str(job.job_id)), None)
        if job_node:
            return job_node
        self._last_node_id += 1
        job_name = job.settings.name if job.settings and job.settings.name else str(job.job_id)
        job_node = MigrationNode(
            node_id=self._last_node_id,
            object_type="JOB",
            object_id=str(job.job_id),
            object_name=job_name,
            object_owner=JobOwnership(self._admin_locator).owner_of(JobInfo.from_job(job)),
        )
        self._nodes[job_node.key] = job_node
        if job.settings and job.settings.job_clusters:
            for job_cluster in job.settings.job_clusters:
                cluster_node = self.register_job_cluster(job_cluster)
                if cluster_node:
                    self._outgoing[job_node.key].add(cluster_node.key)
        return job_node

    def register_job_cluster(self, cluster: jobs.JobCluster) -> MigrationNode | None:
        if cluster.new_cluster:
            return None
        return self.register_cluster(cluster.job_cluster_key)

    def register_cluster(self, cluster_id: str) -> MigrationNode:
        cluster_node = self._nodes.get(("CLUSTER", cluster_id), None)
        if cluster_node:
            return cluster_node
        details = self._ws.clusters.get(cluster_id)
        object_name = details.cluster_name if details and details.cluster_name else cluster_id
        self._last_node_id += 1
        cluster_node = MigrationNode(
            node_id=self._last_node_id,
            object_type="CLUSTER",
            object_id=cluster_id,
            object_name=object_name,
            object_owner=ClusterOwnership(self._admin_locator).owner_of(ClusterInfo.from_cluster_details(details)),
        )
        self._nodes[cluster_node.key] = cluster_node
        # TODO register warehouses and policies
        return cluster_node

    def generate_steps(self) -> Iterable[MigrationStep]:
        """Generate the migration steps.

        An adapted version of the Kahn topological sort is implemented. The differences are as follows:
        - We want the same step number for all nodes with same dependency depth. Therefore, instead of pushing to a
          queue, we rebuild it once all leaf nodes are processed (these are transient leaf nodes i.e. they only become
          leaf during processing)
        - We handle cyclic dependencies (implemented in PR #3009)
        """
        # pre-compute incoming keys for best performance of self._required_step_ids
        incoming_keys = self._collect_incoming_keys()
        incoming_counts = self.compute_incoming_counts(incoming_keys)
        key_queue = self._create_key_queue(incoming_counts)
        key = key_queue.get()
        step_number = 1
        sorted_steps: list[MigrationStep] = []
        while key is not None:
            required_step_ids = sorted(self._get_required_step_ids(incoming_keys[key]))
            step = self._nodes[key].as_step(step_number, required_step_ids)
            sorted_steps.append(step)
            # Update queue priorities
            for dependency_key in self._outgoing[key]:
                incoming_counts[dependency_key] -= 1
                key_queue.update(incoming_counts[dependency_key], dependency_key)
            step_number += 1
            key = key_queue.get()
        return sorted_steps

    def _collect_incoming_keys(self) -> dict[tuple[str, str], set[tuple[str, str]]]:
        result: dict[tuple[str, str], set[tuple[str, str]]] = defaultdict(set)
        for source, outgoing in self._outgoing.items():
            for target in outgoing:
                result[target].add(source)
        return result

    def _get_required_step_ids(self, required_step_keys: set[tuple[str, str]]) -> Iterable[int]:
        for source_key in required_step_keys:
            yield self._nodes[source_key].node_id

    def compute_incoming_counts(
        self, incoming: dict[tuple[str, str], set[tuple[str, str]]]
    ) -> dict[tuple[str, str], int]:
        result = defaultdict(int)
        for node_key in self._nodes:
            result[node_key] = len(incoming[node_key])
        return result

    @staticmethod
    def _create_key_queue(incoming_counts: dict[tuple[str, str], int]) -> PriorityQueue:
        """Create a priority queue given the keys and their incoming counts.

        A lower number means it is pulled from the queue first, i.e. the key with the lowest number of keys is retrieved
        first.
        """
        priority_queue = PriorityQueue()
        for node_key, incoming_count in incoming_counts.items():
            priority_queue.put(incoming_count, node_key)
        return priority_queue

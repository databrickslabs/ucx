from __future__ import annotations

import heapq
import itertools
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service import jobs

from databricks.labs.ucx.assessment.clusters import ClusterOwnership, ClusterInfo
from databricks.labs.ucx.assessment.jobs import JobOwnership, JobInfo
from databricks.labs.ucx.framework.owners import AdministratorLocator
from databricks.labs.ucx.source_code.graph import DependencyGraph, DependencyProblem


@dataclass
class MigrationStep:
    step_id: int
    """Globally unique id."""

    step_number: int
    """The position in the migration sequence."""

    object_type: str
    """Object type. Together with `attr:object_id` a unique identifier."""

    object_id: str
    """Object id. Together with `attr:object_id` a unique identifier."""

    object_name: str
    """Object name, more human friendly than `attr:object_id`."""

    object_owner: str
    """Object owner."""

    required_step_ids: list[int]
    """The step ids that should be completed before this step is started."""


MigrationNodeKey = tuple[str, str]


@dataclass(frozen=True)
class MigrationNode:
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


@dataclass
class MaybeMigrationNode:
    node: MigrationNode | None
    problems: list[DependencyProblem]

    @property
    def failed(self) -> bool:
        return len(self.problems) > 0


# We expect `tuple[int, int, MigrationNode | str]`
# for `[priority, counter, MigrationNode | PriorityQueue._REMOVED | PriorityQueue_UPDATED]`
# but we use list for the required mutability
QueueEntry = list[int | MigrationNode | str]


class PriorityQueue:
    """A migration node priority queue.

    Note:
        This implementation does not support threading safety as that is not required.

    Source:
        See https://docs.python.org/3/library/heapq.html#priority-queue-implementation-notes on the changes below
        to handle priority changes in the task. Also, the _UPDATED marker is introduced to avoid updating removed nodes.
    """

    _REMOVED = "<removed>"  # Mark removed items

    def __init__(self):
        self._entries: list[QueueEntry] = []
        self._entry_finder: dict[MigrationNode, QueueEntry] = {}
        self._counter = itertools.count()  # Tiebreaker with equal priorities, then "first in, first out"

    def put(self, priority: int, task: MigrationNode) -> None:
        """Put or update task in the queue.

        The lowest priority is retrieved from the queue first.
        """
        if task in self._entry_finder:
            self._remove(task)
        count = next(self._counter)
        entry = [priority, count, task]
        self._entry_finder[task] = entry
        heapq.heappush(self._entries, entry)

    def get(self) -> MigrationNode | None:
        """Gets the tasks with lowest priority."""
        while self._entries:
            _, _, task = heapq.heappop(self._entries)
            if task == self._REMOVED:
                continue
            assert isinstance(task, MigrationNode)
            self._remove(task)
            # Ignore type because heappop returns Any, while we know it is an QueueEntry
            return task
        return None

    def _remove(self, task: MigrationNode) -> None:
        """Remove a task from the queue."""
        entry = self._entry_finder.pop(task)
        entry[2] = self._REMOVED


class MigrationSequencer:
    """Sequence the migration dependencies in order to execute the migration.

    Similar to the other graph logic, we first build the graph by registering dependencies, then we analyse the graph.
    Analysing the graph in this case means: computing the migration sequence in `meth:generate_steps`.
    """

    def __init__(self, ws: WorkspaceClient, admin_locator: AdministratorLocator):
        self._ws = ws
        self._admin_locator = admin_locator
        self._counter = itertools.count()
        self._nodes: dict[MigrationNodeKey, MigrationNode] = {}  # TODO: Update to MaybeMigrationNode
        self._outgoing: dict[MigrationNodeKey, set[MigrationNode]] = defaultdict(set)

    def _register_workflow_task(self, task: jobs.Task, parent: MigrationNode) -> MaybeMigrationNode:
        """Register a workflow task.

        Args:
            task : jobs.Task
                The task to register
            parent : MigrationNode
                The migration node for the parent job
        """
        task_id = f"{parent.key}/{task.task_key}"
        task_node = self._nodes.get(("TASK", task_id), None)
        if task_node:
            return MaybeMigrationNode(task_node, [])
        task_node = MigrationNode(
            node_id=next(self._counter),
            object_type="TASK",
            object_id=task_id,
            object_name=task.task_key,
            object_owner=parent.object_owner,  # No task owner so use parent job owner
        )
        self._nodes[task_node.key] = task_node
        if task.existing_cluster_id:
            maybe_cluster_node = self.register_cluster(task.existing_cluster_id)
            if maybe_cluster_node.node:
                self._outgoing[task_node.key].add(maybe_cluster_node.node)
        # TODO: register `job_cluster_key
        return MaybeMigrationNode(task_node, [])

    def register_workflow_job(self, job: jobs.Job) -> MigrationNode:
        job_node = self._nodes.get(("JOB", str(job.job_id)), None)
        if job_node:
            return job_node
        job_name = job.settings.name if job.settings and job.settings.name else str(job.job_id)
        job_node = MigrationNode(
            node_id=next(self._counter),
            object_type="JOB",
            object_id=str(job.job_id),
            object_name=job_name,
            object_owner=JobOwnership(self._admin_locator).owner_of(JobInfo.from_job(job)),
        )
        self._nodes[job_node.key] = job_node
        if job.settings and job.settings.job_clusters:
            for job_cluster in job.settings.job_clusters:
                maybe_cluster_node = self.register_job_cluster(job_cluster)
                if maybe_cluster_node.node:
                    self._outgoing[job_node.key].add(maybe_cluster_node.node)
            for task in job.settings.tasks or []:
                maybe_task_node = self._register_workflow_task(task, job)
                if maybe_task_node.node:
                    self._outgoing[job_node.key] = maybe_task_node.node
        return job_node

    def register_job_cluster(self, cluster: jobs.JobCluster) -> MaybeMigrationNode:
        if cluster.new_cluster:
            return MaybeMigrationNode(None, [])
        return self.register_cluster(cluster.job_cluster_key)

    def register_cluster(self, cluster_id: str) -> MaybeMigrationNode:
        node_seen = self._nodes.get(("CLUSTER", cluster_id), None)
        if node_seen:
            return MaybeMigrationNode(node_seen, [])
        try:
            details = self._ws.clusters.get(cluster_id)
        except DatabricksError:
            message = f"Could not find cluster: {cluster_id}"
            return MaybeMigrationNode(None, [DependencyProblem('cluster-not-found', message)])
        object_name = details.cluster_name if details and details.cluster_name else cluster_id
        cluster_node = MigrationNode(
            node_id=next(self._counter),
            object_type="CLUSTER",
            object_id=cluster_id,
            object_name=object_name,
            object_owner=ClusterOwnership(self._admin_locator).owner_of(ClusterInfo.from_cluster_details(details)),
        )
        self._nodes[cluster_node.key] = cluster_node
        # TODO register warehouses and policies
        return MaybeMigrationNode(cluster_node, [])

    def generate_steps(self) -> Iterable[MigrationStep]:
        """Generate the migration steps.

        An adapted version of the Kahn topological sort is implemented. The differences are as follows:
        - We want the same step number for all nodes with same dependency depth. Therefore, instead of pushing to a
          queue, we rebuild it once all leaf nodes are processed (these are transient leaf nodes i.e. they only become
          leaf during processing)
        - We handle cyclic dependencies (implemented in PR #3009)
        """
        # pre-compute incoming keys for best performance of self._required_step_ids
        incoming = self._invert_outgoing_to_incoming()
        queue = self._create_node_queue(incoming)
        seen = set[MigrationNode]()
        node = queue.get()
        ordered_steps: list[MigrationStep] = []
        while node is not None:
            step = node.as_step(len(ordered_steps), sorted(n.node_id for n in incoming[node.key]))
            ordered_steps.append(step)
            seen.add(node)
            # Update the queue priority as if the migration step was completed
            for dependency in self._outgoing[node.key]:
                if dependency in seen:
                    continue
                priority = len(incoming[dependency.key] - seen)
                queue.put(priority, dependency)
            node = queue.get()
        return ordered_steps

    def _invert_outgoing_to_incoming(self) -> dict[MigrationNodeKey, set[MigrationNode]]:
        result: dict[MigrationNodeKey, set[MigrationNode]] = defaultdict(set)
        for node_key, outgoing_nodes in self._outgoing.items():
            for target in outgoing_nodes:
                result[target.key].add(self._nodes[node_key])
        return result

    def _create_node_queue(self, incoming: dict[MigrationNodeKey, set[MigrationNode]]) -> PriorityQueue:
        """Create a priority queue for their nodes using the incoming count as priority.

        A lower number means it is pulled from the queue first, i.e. the key with the lowest number of keys is retrieved
        first.
        """
        priority_queue = PriorityQueue()
        for node_key, node in self._nodes.items():
            priority_queue.put(len(incoming[node_key]), node)
        return priority_queue

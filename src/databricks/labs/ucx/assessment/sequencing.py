from __future__ import annotations

import heapq
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

from databricks.labs.ucx.assessment.clusters import ClusterOwnership, ClusterInfo
from databricks.labs.ucx.assessment.jobs import JobOwnership, JobInfo
from databricks.labs.ucx.framework.owners import AdministratorLocator
from databricks.labs.ucx.source_code.graph import DependencyGraph


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
    _UPDATED = "<updated>"  # Mark updated items

    def __init__(self):
        self._entries: list[QueueEntry] = []
        self._entry_finder: dict[MigrationNode, QueueEntry] = {}
        self._counter = 0  # Tiebreaker with equal priorities, then "first in, first out"

    def put(self, priority: int, task: MigrationNode) -> None:
        """Put or update task in the queue.

        The lowest priority is retrieved from the queue first.
        """
        if task in self._entry_finder:
            raise KeyError(f"Use `:meth:update` to update existing task: {task}")
        entry: QueueEntry = [priority, self._counter, task]
        self._entry_finder[task] = entry
        heapq.heappush(self._entries, entry)
        self._counter += 1

    def get(self) -> MigrationNode | None:
        """Gets the tasks with lowest priority."""
        while self._entries:
            _, _, task = heapq.heappop(self._entries)
            if task in (self._REMOVED, self._UPDATED):
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

    def update(self, priority: int, task: MigrationNode) -> None:
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
        self._outgoing: dict[MigrationNodeKey, set[MigrationNode]] = defaultdict(set)

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
        self._outgoing[task_node.key].add(job_node)
        if task.existing_cluster_id:
            cluster_node = self.register_cluster(task.existing_cluster_id)
            if cluster_node:
                self._outgoing[task_node.key].add(cluster_node)
                # also make the cluster dependent on the job
                self._outgoing[job_node.key].add(cluster_node)
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
                    self._outgoing[job_node.key].add(cluster_node)
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
                priority = len(incoming[dependency.key] - seen)
                queue.update(priority, dependency)
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
        for node_key, incoming_nodes in incoming.items():
            priority_queue.put(len(incoming_nodes), self._nodes[node_key])
        return priority_queue

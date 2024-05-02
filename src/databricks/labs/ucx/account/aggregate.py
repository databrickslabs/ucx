import collections
import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from functools import cached_property

from databricks.labs.lsql import Row

from databricks.labs.ucx.account.workspaces import AccountWorkspaces
from databricks.labs.blueprint.installation import NotInstalled

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex, MigrationStatus
from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.queries import FromTable

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass
class AssessmentObject:
    workspace_id: int
    object_type: str
    object_id: str
    failures: list[str]


class AccountAggregate:
    def __init__(self, account_workspaces: AccountWorkspaces):
        self._account_workspaces = account_workspaces

    @cached_property
    def _workspace_contexts(self):
        # pylint: disable-next=import-outside-toplevel
        from databricks.labs.ucx.contexts.cli_command import WorkspaceContext

        contexts = []
        for workspace_client in self._account_workspaces.workspace_clients():
            contexts.append(WorkspaceContext(workspace_client))
        return contexts

    def _federated_ucx_query(self, query: str) -> Iterable[tuple[int, Row]]:
        """Modifies a query with a workspace-specific UCX schema and executes it on each workspace,
        yielding a tuple of workspace_id and Row. This means that you don't have to specify a database in the query,
        as it will be replaced with the UCX schema for each workspace. Use this method to aggregate results across
        all workspaces, where UCX is installed.

        At the moment, it's done sequentially, which is theoretically inefficient, but the number of workspaces is
        expected to be small. If this becomes a performance bottleneck, we can optimize it later via Threads.strict()
        """
        empty_index = MigrationIndex([])
        for ctx in self._workspace_contexts:
            workspace_id = ctx.workspace_client.get_workspace_id()
            try:
                # use already existing code to replace tables in the query, assuming that UCX database is in HMS
                # TODO: this worked, but changing to check CurrentSessionState
                # empty_index = MigrationIndex([MigrationStatus(ctx.config.inventory_database, "objects", None, ctx.config.inventory_database, "objects", "test")])
                from_table = FromTable(empty_index, CurrentSessionState(schema=ctx.config.inventory_database))
                logger.info(f"Querying Schema {ctx.config.inventory_database}")

                workspace_specific_query = from_table.apply(query)
                for row in ctx.sql_backend.fetch(workspace_specific_query):
                    yield workspace_id, row
            except NotInstalled:
                logger.warning(f"Workspace {workspace_id} does not have UCX installed")
            # TODO: Add this exception handling
            # except NotSuchTableException as e:
            #     logger.warning(f"Workspace {workspace_id} does not have the required table: {e.table_name}")

    @cached_property
    def _aggregate_objects(self) -> list[AssessmentObject]:
        objects = []
        # view is defined in src/databricks/labs/ucx/queries/views/objects.sql
        for workspace_id, row in self._federated_ucx_query('SELECT * FROM objects'):
            objects.append(AssessmentObject(workspace_id, row.object_type, row.object_id, json.loads(row.failures)))
        return objects

    def readiness_report(self):
        logger.info("Generating readiness report")
        all_objects = 0
        incompatible_objects = 0
        failures = collections.defaultdict(list)

        for obj in self._aggregate_objects:
            all_objects += 1
            has_failures = False
            for failure in obj.failures:
                failures[failure].append(obj)
                has_failures = True
            if has_failures:
                incompatible_objects += 1
        compatibility = (1 - incompatible_objects / all_objects if all_objects > 0 else 0) * 100
        logger.info(f"UC compatibility: {compatibility}% ({incompatible_objects}/{all_objects})")

        for failure, objects in failures.items():
            logger.info(f"{failure}: {len(objects)} objects")


import collections
import json
import logging
from dataclasses import dataclass
from functools import cached_property

from databricks.labs.ucx.account.workspaces import AccountWorkspaces
from databricks.labs.blueprint.installation import NotInstalled


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

    @cached_property
    def _aggregate_objects(self) -> list[AssessmentObject]:
        objects = []
        # this is theoretically inefficient, but the number of workspaces is expected to be small. If this is a
        # performance bottleneck, we can optimize it later via Threads.strict()
        for ctx in self._workspace_contexts:
            try:
                workspace_id = ctx.workspace_client.get_workspace_id()
                logger.info(f"Assessing workspace {workspace_id}")

                # view is defined in src/databricks/labs/ucx/queries/views/objects.sql
                for row in ctx.sql_backend.fetch(f'SELECT * FROM {ctx.config.inventory_database}.objects'):
                    objects.append(AssessmentObject(workspace_id, row.object_type, row.object_id, json.loads(row.failures)))
            except NotInstalled:
                logger.warning(f"Workspace {ctx.workspace_client.get_workspace_id()} does not have UCX installed")
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

        # TODO: output failures in file
        # for failure, objects in failures.items():
        #     logger.info(f"{failure}: {len(objects)} objects")

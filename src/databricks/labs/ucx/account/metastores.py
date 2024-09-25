import logging

from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import AccountClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.settings import DefaultNamespaceSetting, StringMessage

logger = logging.getLogger(__name__)


class AccountMetastores:
    def __init__(
        self,
        account_client: AccountClient,
    ):
        self._ac = account_client

    def show_all_metastores(self, workspace_id: str | None = None):
        location = None
        if workspace_id:
            logger.info(f"Workspace ID: {workspace_id}")
            location = self._get_region(int(workspace_id))
        logger.info("Matching metastores are:")
        for metastore in self._get_all_metastores(location).keys():
            logger.info(metastore)

    def assign_metastore(
        self,
        prompts: Prompts,
        workspace_id: int,
        *,
        metastore_id: str | None = None,
        default_catalog: str | None = None,
    ):
        if metastore_id is None:
            # search for all matching metastores
            metastore_choices = self._get_all_metastores(self._get_region(workspace_id))
            if len(metastore_choices) == 0:
                raise ValueError(f"No matching metastore found for workspace: {workspace_id}")
            # if there are multiple matches, prompt users to select one
            if len(metastore_choices) > 1:
                metastore_id = prompts.choice_from_dict(
                    "Multiple metastores found, please select one:", metastore_choices
                )
            else:
                metastore_id = list(metastore_choices.values())[0]
        self._ac.metastore_assignments.create(workspace_id, metastore_id)
        # set the default catalog using the default_namespace setting API
        if default_catalog is not None:
            self._set_default_catalog(workspace_id, default_catalog)

    def _get_region(self, workspace_id: int) -> str | None:
        workspace = self._ac.workspaces.get(workspace_id)
        if self._ac.config.is_aws:
            return workspace.aws_region
        return workspace.location

    def _get_all_workspaces(self) -> dict[str, int]:
        output = dict[str, int]()
        for workspace in self._ac.workspaces.list():
            if workspace.workspace_id:
                output[f"{workspace.workspace_name} - {workspace.workspace_id}"] = workspace.workspace_id
        return dict(sorted(output.items()))

    def _get_all_metastores(self, location: str | None = None) -> dict[str, str]:
        output = dict[str, str]()
        for metastore in self._ac.metastores.list():
            if location is None or metastore.region == location:
                output[f"{metastore.name} - {metastore.metastore_id}"] = str(metastore.metastore_id)
        return dict(sorted(output.items()))

    def _set_default_catalog(self, workspace_id: int, default_catalog: str):
        if default_catalog == "":
            return
        workspace = self._ac.workspaces.get(int(workspace_id))
        default_namespace = self._ac.get_workspace_client(workspace).settings.default_namespace
        # needs to get the etag first, before patching the setting
        try:
            etag = default_namespace.get().etag
        except NotFound as err:
            # if not found, the etag is returned in the header
            etag = err.details[0].metadata.get("etag")
        default_namespace.update(
            allow_missing=True,
            field_mask="namespace.value",
            setting=DefaultNamespaceSetting(etag=etag, namespace=StringMessage(default_catalog)),
        )

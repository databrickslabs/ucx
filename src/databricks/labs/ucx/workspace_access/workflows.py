import logging

from databricks.labs.ucx.assessment.workflows import Assessment
from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from databricks.labs.ucx.framework.tasks import Workflow, job_task
from databricks.labs.ucx.hive_metastore.verification import VerifyHasMetastore

logger = logging.getLogger(__name__)


class GroupMigration(Workflow):
    def __init__(self):
        super().__init__('migrate-groups')

    @job_task(depends_on=[Assessment.crawl_groups])
    def rename_workspace_local_groups(self, ctx: RuntimeContext):
        """Renames workspace local groups by adding `ucx-renamed-` prefix."""
        verify_has_metastore = VerifyHasMetastore(ctx.workspace_client)
        if verify_has_metastore.verify_metastore():
            logger.info("Metastore exists in the workspace")
        ctx.group_manager.rename_groups()

    @job_task(depends_on=[rename_workspace_local_groups])
    def reflect_account_groups_on_workspace(self, ctx: RuntimeContext):
        """Adds matching account groups to this workspace. The matching account level group(s) must preexist(s) for this
        step to be successful. This process does not create the account level group(s)."""
        ctx.group_manager.reflect_account_groups_on_workspace()

    @job_task(depends_on=[reflect_account_groups_on_workspace], job_cluster="tacl")
    def apply_permissions_to_account_groups(self, ctx: RuntimeContext):
        """Fourth phase of the workspace-local group migration process. It does the following:
          - Assigns the full set of permissions of the original group to the account-level one

        It covers local workspace-local permissions for all entities: Legacy Table ACLs, Entitlements,
        AWS instance profiles, Clusters, Cluster policies, Instance Pools, Databricks SQL warehouses, Delta Live
        Tables, Jobs, MLflow experiments, MLflow registry, SQL Dashboards & Queries, SQL Alerts, Token and Password usage
        permissions, Secret Scopes, Notebooks, Directories, Repos, Files.

        See [interactive tutorial here](https://app.getreprise.com/launch/myM3VNn/)."""
        migration_state = ctx.group_manager.get_migration_state()
        if len(migration_state.groups) == 0:
            logger.info("Skipping group migration as no groups were found.")
            return
        ctx.permission_manager.apply_group_permissions(migration_state)

    @job_task(job_cluster="tacl")
    def validate_groups_permissions(self, ctx: RuntimeContext):
        """Validate that all the crawled permissions are applied correctly to the destination groups."""
        ctx.permission_manager.verify_group_permissions()

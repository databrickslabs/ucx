import logging

from databricks.labs.ucx.assessment.workflows import Assessment
from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from databricks.labs.ucx.framework.tasks import Workflow, job_task

logger = logging.getLogger(__name__)


class LegacyGroupMigration(Workflow):
    def __init__(self):
        super().__init__('migrate-groups-legacy')

    @job_task(job_cluster="user_isolation")
    def verify_metastore_attached(self, ctx: RuntimeContext):
        """Verifies if a metastore is attached to this workspace. If not, the workflow will fail.

        Account level groups are only available when a metastore is attached to the workspace.
        """
        if not ctx.config.use_legacy_permission_migration:
            logger.error("Use `migrate-groups` job, or set `use_legacy_permission_migration: true` in config.yml.")
            return
        ctx.verify_has_metastore.verify_metastore()

    @job_task(depends_on=[Assessment.crawl_groups, verify_metastore_attached])
    def rename_workspace_local_groups(self, ctx: RuntimeContext):
        """Renames workspace local groups by adding `db-temp-` prefix."""
        if not ctx.config.use_legacy_permission_migration:
            logger.error("Use `migrate-groups` job, or set `use_legacy_permission_migration: true` in config.yml.")
            return
        ctx.group_manager.rename_groups()

    @job_task(depends_on=[rename_workspace_local_groups])
    def reflect_account_groups_on_workspace(self, ctx: RuntimeContext):
        """Adds matching account groups to this workspace. The matching account level group(s) must preexist(s) for this
        step to be successful. This process does not create the account level group(s)."""
        if not ctx.config.use_legacy_permission_migration:
            logger.error("Use `migrate-groups` job, or set `use_legacy_permission_migration: true` in config.yml.")
            return
        ctx.group_manager.reflect_account_groups_on_workspace()

    @job_task(job_cluster="tacl")
    def setup_tacl(self, ctx: RuntimeContext):
        """(Optimization) Allow the TACL job cluster to be started while we're verifying the prerequisites for
        refreshing everything."""

    @job_task(depends_on=[reflect_account_groups_on_workspace, setup_tacl], job_cluster="tacl")
    def apply_permissions_to_account_groups(self, ctx: RuntimeContext):
        """Fourth phase of the workspace-local group migration process. It does the following:
          - Assigns the full set of permissions of the original group to the account-level one

        It covers local workspace-local permissions for all entities: Legacy Table ACLs, Entitlements,
        AWS instance profiles, Clusters, Cluster policies, Instance Pools, Databricks SQL warehouses, Delta Live
        Tables, Jobs, MLflow experiments, MLflow registry, SQL Dashboards & Queries, SQL Alerts, Token and Password usage
        permissions, Secret Scopes, Notebooks, Directories, Repos, Files.

        See [interactive tutorial here](https://app.getreprise.com/launch/myM3VNn/)."""
        if not ctx.config.use_legacy_permission_migration:
            logger.error("Use `migrate-groups` job, or set `use_legacy_permission_migration: true` in config.yml.")
            return
        migration_state = ctx.group_manager.get_migration_state()
        if len(migration_state.groups) == 0:
            logger.info("Skipping group migration as no groups were found.")
            return
        ctx.permission_manager.apply_group_permissions(migration_state)

    @job_task(depends_on=[apply_permissions_to_account_groups], job_cluster="tacl")
    def validate_groups_permissions(self, ctx: RuntimeContext):
        """Validate that all the crawled permissions are applied correctly to the destination groups."""
        if not ctx.config.use_legacy_permission_migration:
            logger.error("Use `migrate-groups` job, or set `use_legacy_permission_migration: true` in config.yml.")
            return
        if not ctx.permission_manager.verify_group_permissions():
            raise ValueError(
                "Some group permissions were not migrated successfully. Wait for an hour then use the "
                "`validate-groups-permissions` workflow to validate the permissions after the API caught up. "
                f"Run `databricks labs ucx logs --workflow '{self._name}' --debug` for more details."
            )


class PermissionsMigrationAPI(Workflow):
    def __init__(self):
        super().__init__('migrate-groups')

    @job_task(job_cluster="user_isolation")
    def verify_metastore_attached(self, ctx: RuntimeContext):
        """Verifies if a metastore is attached to this workspace. If not, the workflow will fail.

        Account level groups are only available when a metastore is attached to the workspace.
        """
        if ctx.config.use_legacy_permission_migration:
            logger.error("Remove `use_legacy_permission_migration: true` from config.yml to run this workflow.")
            return
        ctx.verify_has_metastore.verify_metastore()

    @job_task(depends_on=[Assessment.crawl_groups, verify_metastore_attached])
    def rename_workspace_local_groups(self, ctx: RuntimeContext):
        """Renames workspace local groups by adding `db-temp-` prefix."""
        if ctx.config.use_legacy_permission_migration:
            logger.error("Remove `use_legacy_permission_migration: true` from config.yml to run this workflow.")
            return
        ctx.group_manager.rename_groups()

    @job_task(depends_on=[rename_workspace_local_groups])
    def reflect_account_groups_on_workspace(self, ctx: RuntimeContext):
        """Adds matching account groups to this workspace. The matching account level group(s) must preexist(s) for this
        step to be successful. This process does not create the account level group(s)."""
        if ctx.config.use_legacy_permission_migration:
            logger.error("Remove `use_legacy_permission_migration: true` from config.yml to run this workflow.")
            return
        ctx.group_manager.reflect_account_groups_on_workspace()

    @job_task(depends_on=[reflect_account_groups_on_workspace])
    def apply_permissions(self, ctx: RuntimeContext):
        """This task uses the new permission migration API which requires enrolment from Databricks.
        Fourth phase of the workspace-local group migration process. It does the following:
          - Assigns the full set of permissions of the original group to the account-level one

        The permission migration is not atomic. If we hit InternalError, it is possible that half the permissions
        have already been migrated over to the account group, and the other half of the permissions are still with
        the workspace local group. Addressing cases like this would require two options that both require manual
        intervention:
          - Deleting all conflicting permissions for the account group, rerun the permission migration between
            the workspace and account group (the workflow is idempotent).
          - Creating a new account group, reverting all the permissions that were migrated over to the old account
            group, and running the workflow again.

        To make things run smoothly, this workflow should never be run on an account group that already has permissions.
        The expectation is that account group has no permissions to begin with.

        It covers local workspace-local permissions for all entities."""
        if ctx.config.use_legacy_permission_migration:
            logger.error("Remove `use_legacy_permission_migration: true` from config.yml to run this workflow.")
            return
        migration_state = ctx.group_manager.get_migration_state()
        if len(migration_state.groups) == 0:
            logger.info("Skipping group migration as no groups were found.")
        elif migration_state.apply_to_renamed_groups(ctx.workspace_client):
            logger.info("Group permission migration completed successfully.")
        else:
            msg = "Permission migration for groups failed; reason unknown."
            raise RuntimeError(msg)


class ValidateGroupPermissions(Workflow):
    def __init__(self):
        super().__init__('validate-groups-permissions')

    @job_task(job_cluster="tacl")
    def validate_groups_permissions(self, ctx: RuntimeContext):
        """Validate that all the crawled permissions are applied correctly to the destination groups."""
        if not ctx.config.use_legacy_permission_migration:
            logger.error("Use `migrate-groups` job, or set `use_legacy_permission_migration: true` in config.yml.")
            return
        if not ctx.permission_manager.verify_group_permissions():
            raise ValueError(
                f"Some group permissions were not migrated successfully. Run `databricks labs ucx logs --workflow '{self._name}' --debug` for more details."
            )


class RemoveWorkspaceLocalGroups(Workflow):
    def __init__(self):
        super().__init__('remove-workspace-local-backup-groups')

    @job_task(depends_on=[LegacyGroupMigration.apply_permissions_to_account_groups])
    def delete_backup_groups(self, ctx: RuntimeContext):
        """Last step of the group migration process. Removes all workspace-level backup groups, along with their
        permissions. Execute this workflow only after you've confirmed that workspace-local migration worked
        successfully for all the groups involved."""
        ctx.group_manager.delete_original_workspace_groups()

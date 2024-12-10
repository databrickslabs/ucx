## Group migration workflow

{{<callout type="warning">}}
You are required to complete the [assessment workflow](docs/reference/workflows/assessment.md) before starting the table migration workflow.
{{</callout>}}

**The group migration workflow does NOT CREATE account groups.** In contrast to account groups, [the (legacy)
workspace-local groups cannot be assigned to additional workspaces or granted access to data in a Unity Catalog
metastore](https://docs.databricks.com/en/admin/users-groups/groups.html#difference-between-account-groups-and-workspace-local-groups).
A Databricks admin [assigns account groups to workspaces](https://docs.databricks.com/en/admin/users-groups/index.html#assign-users-to-workspaces)
using [identity federation](https://docs.databricks.com/en/admin/users-groups/index.html#enable-identity-federation)
to manage groups from a single place: your Databricks account. We expect UCX users to create account groups
centrally while most other Databricks resources that UCX touches are scoped to a single workspace.
If you do not have account groups matching the workspace in which UCX is installed, please
run [`create-account-groups` command](#create-account-groups-command) before running the group migration workflow.

The group migration workflow is designed to migrate workspace-local groups to account-level groups. It verifies if
the necessary groups are available to the workspace with the correct permissions, and removes unnecessary groups and
permissions. The group migration workflow depends on the output of the assessment workflow, thus, should only be
executed after a successful run of the assessment workflow. The group migration workflow may be executed multiple times.

1. `verify_metastore_attached`: Verifies if a metastore is attached. Account level groups are only available when
    a metastore is attached. [See `assign-metastore` command.](#assign-metastore-command)
2. `rename_workspace_local_groups`: This task renames workspace-local groups by adding a `ucx-renamed-` prefix. This
   step is taken to avoid conflicts with account groups that may have the same name as workspace-local groups.
3. `reflect_account_groups_on_workspace`: This task adds matching account groups to this workspace. The matching account
   groups must exist for this step to be successful. This step is necessary to ensure that the account groups
   are available in the workspace for assigning permissions.
4. `apply_permissions_to_account_groups`: This task assigns the full set of permissions of the original group to the
   account-level one. This step is necessary to ensure that the account-level groups have the necessary permissions to
   manage the entities in the workspace. It covers workspace-local permissions for all entities including:
   - Legacy Table ACLs
   - Entitlements
   - AWS instance profiles
   - Clusters
   - Cluster policies
   - Instance Pools
   - Databricks SQL warehouses
   - Delta Live Tables
   - Jobs
   - MLflow experiments
   - MLflow registry
   - SQL Dashboards & Queries
   - SQL Alerts
   - Token and Password usage permissions
   - Secret Scopes
   - Notebooks
   - Directories
   - Repos
   - Files
5. `validate_groups_permissions`: This task validates that all the crawled permissions are applied correctly to the
   destination groups.

After successfully running the group migration workflow:
1. Use [`validate-groups-membership` command](#validate-groups-membership-command) for extra confidence the newly created
   account level groups are considered to be valid.
2. Run the [`remove-workspace-local-backup-grups`](#validate-groups-membership-command) to remove workspace-level backup
   groups, along with their permissions. This should only be executed after confirming that the workspace-local
   migration worked successfully for all the groups involved. This step is necessary to clean up the workspace and
   remove any unnecessary groups and permissions.
3. Proceed to the [table migration process](#Table-Migration).

For additional information see:
- The [detailed design](docs/dev/implementation/local-group-migration.md) of thie group migration workflow.
- The [migration process diagram](#migration-process) showing the group migration workflow in context of the whole
  migration process.



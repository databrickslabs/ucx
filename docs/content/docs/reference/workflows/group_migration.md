---
title: "Group Migration Workflow"
linkTitle: "Group Migration"
---

{{<callout type="warning">}}
You are required to complete the [assessment workflow](docs/reference/workflows/assessment.md) before starting the table migration workflow.
{{</callout>}}

## Pre-read 

Please check [this document](https://docs.databricks.com/en/admin/users-groups/groups.html#difference-between-account-groups-and-workspace-local-groups) to understand the difference between account-level and workspace-level groups. 

The main outcome from the document above is that workspace-level groups cannot be used to manage permissions on Unity Catalog objects. This is because the Unity Catalog is a shared resource across all workspaces in the account, and workspace-level groups are not shared across workspaces.

Therefore, to simplify permission management (both on Unity Catalog objects and other workspace objects), we recommend using account-level groups. This document describes the workflow that is used to migrate workspace-level groups to account-level groups.

## Account group creation

{{<callout type="info">}}
The group migration *workflow* does NOT CREATE account groups.

We recommend using [identity federation](https://docs.databricks.com/en/admin/users-groups/index.html#enable-identity-federation) to centrally provision groups from your identity provider.

Alternatively, you can create groups by using the [`create-account-groups` command](docs/reference/commands.md#create-account-groups) before running the group migration workflow. 

{{</callout>}}

## The workflow

The group migration workflow is designed to migrate workspace-local groups to account-level groups. 


It verifies if the necessary groups are available to the workspace with the correct permissions, and removes unnecessary groups and permissions. The group migration workflow depends on the output of the **assessment workflow**, thus, should only be
executed after a successful run of the assessment workflow.

{{<callout type="info">}}
The group migration workflow may be executed multiple times. If you accidentially created new workspace-local groups, you can run the group migration workflow again to migrate the new groups.
{{</callout>}}

Workflow consists of the following tasks:

{{% steps %}}

### Verify Metastore Attached
Verifies if a metastore is attached. Account level groups are only available when
a metastore is attached. [See `assign-metastore` command.](docs/reference/commands.md#assign-metastore)

### Rename Workspace Local Groups
This task renames workspace-local groups by adding a `ucx-renamed-` prefix. This
step is taken to avoid conflicts with account groups that may have the same name as workspace-local groups.

### Reflect Account Groups on Workspace
This task adds matching account groups to this workspace. The matching account groups must exist for this step to be successful. This step is necessary to ensure that the account groups are available in the workspace for assigning permissions.

### Apply Permissions to Account Groups
This task assigns the full set of permissions of the original group to the account-level one. This step is necessary to ensure that the account-level groups have the necessary permissions to manage the entities in the workspace. 

{{% details title="Click here to see all workspace-level entities" closed="true" %}}
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
{{% /details %}}

### Validate Groups Permissions
This task validates that all the crawled permissions are applied correctly to the destination groups.

{{% /steps %}}

You can trigger the workflow from the Workspace UI. 

## Post-migration steps

After successfully running the group migration workflow:
1. Use [`validate-groups-membership` command](docs/reference/commands.md#validate-groups-membership) for extra confidence the newly created
   account level groups are considered to be valid.
2. Run the [`remove-workspace-local-backup-grups`](docs/reference/commands.md#validate-groups-membership) to remove workspace-level backup
   groups, along with their permissions. This should only be executed after confirming that the workspace-local
   migration worked successfully for all the groups involved. This step is necessary to clean up the workspace and
   remove any unnecessary groups and permissions.
3. Proceed to the [table migration process](docs/process/table_migration.md).


## Additional resources

For more information, see:
- The [detailed design](docs/dev/implementation/local_group_migration.md) of thie group migration workflow.
- The [migration process diagram](docs/process/overview.md#diagram) showing the group migration workflow in context of the whole
  migration process.



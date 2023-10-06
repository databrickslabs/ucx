# Permissions migration logic and data structures

During the UC adoption, it's critical to move the groups from the workspace to account level.

To deliver this migration, the following steps are performed:

| Step description                                                                                                                                                                                                                                                                                       | Relevant API method                                      |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------|
| A set of groups to be migrated is identified (either via `groups.selected` config property, or automatically).<br/>Group existence is verified against the account level.<br/>**If there is no group on the account level, an error is thrown.**<br/>Backup groups are created on the workspace level. | `toolkit.prepare_groups_in_environment()`                |
| Inventory table is cleaned up.                                                                                                                                                                                                                                                                         | `toolkit.cleanup_inventory_table()`                      |
| Workspace local group permissions are inventorized and saved into a Delta Table.                                                                                                                                                                                                                       | `toolkit.inventorize_permissions()`                      |
| Backup groups are entitled with permissions from the inventory table.                                                                                                                                                                                                                                  | `toolkit.apply_permissions_to_backup_groups()`           |
| Workspace-level groups are deleted.  Account-level groups are granted with access to the workspace.<br/>Workspace-level entitlements are synced from backup groups to newly added account-level groups.                                                                                                | `toolkit.replace_workspace_groups_with_account_groups()` |
| Account-level groups are entitled with workspace-level permissions from the inventory table.                                                                                                                                                                                                           | `toolkit.apply_permissions_to_account_groups()`          |
| Backup groups are deleted                                                                                                                                                                                                                                                                              | `toolkit.delete_backup_groups()`                         |
| Inventory table is cleaned up. This step is optional.                                                                                                                                                                                                                                                  | `toolkit.cleanup_inventory_table()`                      |

> Please note that inherited permissions will not be inventorized / migrated. We only cover direct permissions.

On a very high-level, the permissions crawling process is split into two steps:

1. collect all existing permissions into a persistent storage - see `workspace_access.AclSupport.get_crawler_tasks`.
2. apply the collected permissions to the target resources - see `workspace_access.AclSupport.get_apply_task`.

We implement `workspace_access.AclSupport` for each supported resource type.

## Logical objects and relevant APIs


### Group level properties (uses SCIM API)

- [x] Entitlements (One of `workspace-access`, `databricks-sql-access`, `allow-cluster-create`, `allow-instance-pool-create`)
- [x] Roles (AWS only)

These are workspace-level properties that are not associated with any specific resource.

Additional info:

- object ID: `group_id`
- listing method: `ws.groups.list`
- get method: `ws.groups.get(group_id)`
- put method: `ws.groups.patch(group_id)`

### Compute infrastructure (uses Permissions API)

- [x] Clusters
- [x] Cluster policies
- [x] Instance pools
- [x] SQL warehouses

These are compute infrastructure resources that are associated with a specific workspace.

Additional info:

- object ID: `cluster_id`, `policy_id`, `instance_pool_id`, `id` (SQL warehouses)
- listing method: `ws.clusters.list`, `ws.cluster_policies.list`, `ws.instance_pools.list`, `ws.warehouses.list`
- get method: `ws.permissions.get(object_id, object_type)`
- put method: `ws.permissions.update(object_id, object_type)`
- get response object type: `databricks.sdk.service.iam.ObjectPermissions`


### Workflows (uses Permissions API)

- [x] Jobs
- [x] Delta Live Tables

These are workflow resources that are associated with a specific workspace.

Additional info:

- object ID: `job_id`, `pipeline_id`
- listing method: `ws.jobs.list`, `ws.pipelines.list`
- get method: `ws.permissions.get(object_id, object_type)`
- put method: `ws.permissions.update(object_id, object_type)`
- get response object type: `databricks.sdk.service.iam.ObjectPermissions`

### ML (uses Permissions API)

- [x] MLflow experiments
- [x] MLflow models

These are ML resources that are associated with a specific workspace.

Additional info:

- object ID: `experiment_id`, `id` (models)
- listing method: custom listing
- get method: `ws.permissions.get(object_id, object_type)`
- put method: `ws.permissions.update(object_id, object_type)`
- get response object type: `databricks.sdk.service.iam.ObjectPermissions`


### SQL (uses SQL Permissions API)

- [x] Alerts
- [x] Dashboards
- [x] Queries

These are SQL resources that are associated with a specific workspace.

Additional info:

- object ID: `id`
- listing method: `ws.alerts.list`, `ws.dashboards.list`, `ws.queries.list`
- get method: `ws.dbsql_permissions.get`
- put method: `ws.dbsql_permissions.set`
- get response object type: `databricks.sdk.service.sql.GetResponse`
- Note that API has no support for UPDATE operation, only PUT (overwrite) is supported.

### Security (uses Permissions API)

- [x] Tokens
- [x] Passwords

These are security resources that are associated with a specific workspace.

Additional info:

- object ID: `tokens` (static value), `passwords` (static value)
- listing method: N/A
- get method: `ws.permissions.get(object_id, object_type)`
- put method: `ws.permissions.update(object_id, object_type)`
- get response object type: `databricks.sdk.service.iam.ObjectPermissions`

### Workspace (uses Permissions API)

- [x] Notebooks
- [x] Directories
- [x] Repos
- [x] Files

These are workspace resources that are associated with a specific workspace.

Additional info:

- object ID: `object_id`
- listing method: custom listing
- get method: `ws.permissions.get(object_id, object_type)`
- put method: `ws.permissions.update(object_id, object_type)`
- get response object type: `databricks.sdk.service.iam.ObjectPermissions`

#### Known issues

- Folder names with forward-slash (`/`) in directory name will be skipped by the inventory. Databricks UI no longer 
allows creating folders with a forward slash. See [this issue](https://github.com/databrickslabs/ucx/issues/230) for 
more details.

### Secrets (uses Secrets API)

- [x] Secrets

These are secrets resources that are associated with a specific workspace.

Additional info:

- object ID: `scope_name`
- listing method: `ws.secrets.list_scopes()`
- get method: `ws.secrets.list_acls(scope_name)`
- put method: `ws.secrets.put_acl`


## AclSupport and serialization logic

Crawlers are expected to return a list of callable functions that will be later used to get the permissions.
Each of these functions shall return a `workspace_access.Permissions` that should be serializable into a Delta Table.
The permission payload differs between different crawlers, therefore each crawler should implement a serialization
method.

## Applier and deserialization logic

Appliers are expected to accept a list of `workspace_access.Permissions` and generate a list of callables that will apply the
given permissions.
Each applier should implement a deserialization method that will convert the raw payload into a typed one.
Each permission item should have a crawler type associated with it, so that the applier can use the correct
deserialization method.

## Relevance identification

Since we save all objects into the permission table, we need to filter out the objects that are not relevant to the
current migration.
We do this inside the `applier`, by returning a `noop` callable if the object is not relevant to the current migration.

## Crawling the permissions

To crawl the permissions, we use the following logic:
1. Go through the list of all crawlers.
2. Get the list of all objects of the given type.
3. For each object, generate a callable that will return a `workspace_access.Permissions`.
4. Execute the callables in parallel
5. Collect the results into a list of `workspace_access.Permissions`.
6. Save the list of `workspace_access.Permissions` into a Delta Table.

## Applying the permissions

To apply the permissions, we use the following logic:

1. Read the Delta Table with raw permissions.
2. Map the items to the relevant `support` object. If no relevant `support` object is found, an exception is raised.
3. Deserialize the items using the relevant applier.
4. Generate a list of callables that will apply the permissions.
5. Execute the callables in parallel.

## Troubleshooting

Below are some useful code snippets that can be useful for troubleshooting. 
Make sure to install [databricks-sdk](https://docs.databricks.com/en/dev-tools/sdk-python.html) on the cluster to run it.

1. Find workspace-local groups that are eligible for migration to the account:
```
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam

ws = WorkspaceClient()

workspace_groups = [
            g
            for g in ws.groups.list(attributes='id,displayName,meta')
            if g.meta.resource_type == "WorkspaceGroup"
        ]
print(f'Found {len(workspace_groups)} workspace-local groups')

account_groups = [
    iam.Group.from_dict(r)
    for r in ws.api_client.do(
        "get",
        "/api/2.0/account/scim/v2/Groups",
        query={"attributes": "id,displayName,meta,members"},
    ).get("Resources", [])
]
account_groups = [g for g in account_groups if g.display_name not in ["users", "admins", "account users"]]
print(f"Found {len(account_groups)} account groups")

ws_group_names = {_.display_name for _ in workspace_groups}
ac_group_names = {_.display_name for _ in account_groups}
group_names = list(ws_group_names.intersection(ac_group_names))
print(f"Found {len(group_names)} groups to migrate")
```

2. Recover workspace-local groups from backup groups from within a debug notebook:

```python
from databricks.labs.ucx.workspace_access.groups import  GroupManager
from databricks.labs.ucx.config import GroupsConfig

group_manager = GroupManager(ws, GroupsConfig(auto=True))
group_manager.ws_local_group_deletion_recovery()
```

3. Recover Table ACL from `$inventory.grants` to `$inventory.permissions`:

```python
from databricks.labs.ucx.hive_metastore import GrantsCrawler, TablesCrawler
from databricks.labs.ucx.workspace_access.manager import PermissionManager
from databricks.labs.ucx.workspace_access.tacl import TableAclSupport
from databricks.labs.ucx.framework.crawlers import RuntimeBackend

sql_backend = RuntimeBackend()
inventory_schema = cfg.inventory_database
tables = TablesCrawler(sql_backend, inventory_schema)
grants = GrantsCrawler(tables)
tacl = TableAclSupport(grants, sql_backend)
permission_manager = PermissionManager(sql_backend, inventory_schema, [tacl])
permission_manager.inventorize_permissions()
```
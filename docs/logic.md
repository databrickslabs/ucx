# Crawler/applier logic and data structures

On a very high-level, the permissions inventorization process is split into two steps:

1. collect all existing permissions into a persistent storage.
2. apply the collected permissions to the target resources.

The first step is performed by the `Crawler` and the second by the `Applier`.

Crawler and applier are intrinsically connected to each other due to SerDe (serialization/deserialization) logic.

We implement separate crawlers and applier for each supported resource type.

Please note that `table ACLs` logic is currently handled separately from the logic described in this document.

## Logical objects and relevant APIs

| Logical group | Resource type      | Object ID                  | Listing method             | Get method                                   | Put method                                      | Get response object type                           | Details                                                                                                               |
|---------------|--------------------|----------------------------|----------------------------|----------------------------------------------|-------------------------------------------------|----------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|
| Group level   | Entitlements       | `group_id`                 | `ws.groups.list`           | `ws.groups.get(group_id)`                    | `ws.groups.patch(group_id)`                     | Custom                                             | (One of `workspace-access`, `databricks-sql-access`, `allow-cluster-create`, `allow-instance-pool-create`)            |
| Group level   | Roles              | `group_id`                 | `ws.groups.list`           | `ws.groups.get(group_id`                     | `ws.groups.patch(group_id`                      | Custom                                             | (AWS only, represents instance profiles)                                                                              |
| Compute infra | Clusters           | `cluster_id`               | `ws.clusters.list`         | `ws.permissions.get(object_id, object_type)` | `ws.permissions.update(object_id, object_type)` | `databricks.sdk.service.iam.ObjectPermissions`     |                                                                                                                       |
| Compute infra | Cluster policies   | `policy_id`                | `ws.cluster_policies.list` | `ws.permissions.get(object_id, object_type)` | `ws.permissions.update(object_id, object_type)` | `databricks.sdk.service.iam.ObjectPermissions`     |                                                                                                                       |
| Compute infra | Instance pools     | `instance_pool_id`         | `ws.instance_pools.list`   | `ws.permissions.get(object_id, object_type)` | `ws.permissions.update(object_id, object_type)` | `databricks.sdk.service.iam.ObjectPermissions`     |                                                                                                                       |
| Compute infra | SQL warehouses     | `id`                       | `ws.warehouses.list`       | `ws.permissions.get(object_id, object_type)` | `ws.permissions.update(object_id, object_type)` | `databricks.sdk.service.iam.ObjectPermissions`     |                                                                                                                       |
| Workflows     | Jobs               | `job_id`                   | `ws.jobs.list`             | `ws.permissions.get(object_id, object_type)` | `ws.permissions.update(object_id, object_type)` | `databricks.sdk.service.iam.ObjectPermissions`     |                                                                                                                       |
| Workflows     | Delta Live Tables  | `pipeline_id`              | `ws.pipelines.list`        | `ws.permissions.get(object_id, object_type)` | `ws.permissions.update(object_id, object_type)` | `databricks.sdk.service.iam.ObjectPermissions`     |                                                                                                                       |
| ML            | MLflow experiments | `experiment_id`            | custom listing             | `ws.permissions.get(object_id, object_type)` | `ws.permissions.update(object_id, object_type)` | `databricks.sdk.service.iam.ObjectPermissions`     |                                                                                                                       |
| ML            | MLflow models      | `id`                       | custom listing             | `ws.permissions.get(object_id, object_type)` | `ws.permissions.update(object_id, object_type)` | `databricks.sdk.service.iam.ObjectPermissions`     |                                                                                                                       |
| SQL           | Alerts             | `id`                       | `ws.alerts.list`           | `ws.dbsql_permissions.get`                   | `ws.dbsql_permissions.set`                      | `databricks.sdk.service.sql.GetResponse`           | Note that API has no support for UPDATE operation, only PUT (overwrite) is supported.                                 |
| SQL           | Dashboards         | `id`                       | `ws.dashboards.list`       | `ws.dbsql_permissions.get`                   | `ws.dbsql_permissions.set`                      | `databricks.sdk.service.sql.GetResponse`           | Note that API has no support for UPDATE operation, only PUT (overwrite) is supported.                                 |
| SQL           | Queries            | `id`                       | `ws.queries.list`          | `ws.dbsql_permissions.get`                   | `ws.dbsql_permissions.set`                      | `databricks.sdk.service.sql.GetResponse`           | Note that API has no support for UPDATE operation, only PUT (overwrite) is supported.                                 |
| Security      | Tokens             | `tokens` - static value    | N/A                        | `ws.token_management.get_token_permissions`  | `ws.token_management.set_token_permissions`     | `databricks.sdk.service.settings.TokenPermissions` | Token permissions are set on the whole workspace level.                                                               |
| Security      | Passwords          | `passwords` - static value | N/A                        | `ws.users.get_password_permissions`          | `ws.users.set_password_permissions`             | `databricks.sdk.service.iam.PasswordPermissions`   | Only for AWS, it defines which groups can log in with passwords. Password permissions are set on the workspace level. |
| Security      | Secrets            | `scope_name`               | `ws.secrets.list_scopes()` | `ws.secrets.list_acls(scope_name)`           | `ws.secrets.put_acl`                            | `databricks.sdk.service.iam.ObjectPermissions`     |                                                                                                                       |
| Workspace     | Notebooks          | `object_id`                | custom listing             | `ws.permissions.get`                         | `ws.permissions.update`                         | `databricks.sdk.service.iam.ObjectPermissions`     |                                                                                                                       |
| Workspace     | Directories        | `object_id`                | custom listing             | `ws.permissions.get`                         | `ws.permissions.update`                         | `databricks.sdk.service.iam.ObjectPermissions`     |                                                                                                                       |
| Workspace     | Repos              | `object_id`                | custom listing             | `ws.permissions.get`                         | `ws.permissions.update`                         | `databricks.sdk.service.iam.ObjectPermissions`     |                                                                                                                       |
| Workspace     | Files              | `object_id`                | custom listing             | `ws.permissions.get`                         | `ws.permissions.update`                         | `databricks.sdk.service.iam.ObjectPermissions`     |                                                                                                                       |

In the implementation we group the objects by the APIs they use to get/set the permissions.

## Crawler and serialization logic

Crawlers are expected to return a list of callable functions that will be later used to get the permissions.
Each of these functions shall return a `PermissionInventoryItem` that should be serializable into a Delta Table.
The permission payload differs between different crawlers, therefore each crawler should implement a serialization
method.

## Applier and deserialization logic

Appliers are expected to accept a list of `PermissionInventoryItem` and generate a list of callables that will apply the
given permissions.
Each applier should implement a deserialization method that will convert the raw payload into a typed one.
Each permission item should have a crawler type associated with it, so that the applier can use the correct
deserialization method.

## Relevance identification

Since we save all objects into the permission table, we need to filter out the objects that are not relevant to the
current migration.
We do this inside the applier, by returning a `noop` callable if the object is not relevant to the current migration.
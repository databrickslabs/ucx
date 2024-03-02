----- BEGIN ---

Workspace Group Migration
---

This feature introduces the ability to migrate groups from workspace level to account level. 
This is achieved through the use of the `MigratedGroup` class, which represents a group that has been migrated from one 
name to another and stores information about the original and new names, as well as the group's members, external ID, 
and roles. The `MigrationState` class holds the state of the migration process and provides methods for getting 
the target principal and temporary name for a given group name.

The `GroupMigrationStrategy` abstract base class defines the interface for a strategy that generates a list 
of `MigratedGroup` objects based on a mapping between workspace and account groups. 
The `MatchingNamesStrategy`, `MatchByExternalIdStrategy`, `RegexSubStrategy`, and `RegexMatchStrategy` classes are 
concrete implementations of this interface.

The `GroupManager` class is a `CrawlerBase` subclass that manages groups in a Databricks workspace. It provides methods 
for renaming groups, reflecting account groups on the workspace, deleting original workspace groups, and validating 
group membership. The class also provides methods for listing workspace and account groups, getting group details, and 
deleting groups.

The `ConfigureGroups` class provides a command-line interface for configuring the group migration process. It prompts 
the user to enter information about the group migration strategy, such as the renamed group prefix, regular expressions 
for matching and substitution, and a list of groups to migrate. The class also provides methods for validating user input 
and setting class variables based on the user's responses.


Introducing the `AclSupport` abstract base class and the `Permissions` dataclass, designed to provide a flexible and extensible interface for managing permissions for different types of objects in a Databricks workspace.

The `AclSupport` class defines the methods required for managing permissions, including:

* `get_crawler_tasks`: A method that returns a list of callables that crawl and return permissions for the objects supported by the `AclSupport` instance. This method should be implemented to provide the necessary logic for crawling permissions for the specific type of object supported.
* `get_apply_task`: A method that returns a callable that applies the permissions for a given `Permissions` object to a destination group, based on the group's migration state. The callable should not have any shared mutable state, ensuring thread safety and reducing the risk of bugs.
* `get_verify_task`: A method that returns a callable that verifies that the permissions for a given `Permissions` object are applied correctly to the destination group. This method can be used to ensure that permissions are applied as expected, helping to improve the reliability and security of your Databricks workspace.
* `object_types`: An abstract method that returns a set of strings representing the object types that the `AclSupport` instance supports. This method should be implemented to provide the necessary information about the object types supported by the `AclSupport` class.

The `Permissions` dataclass is used to represent the permissions for a specific object type and ID. The dataclass includes a `raw` attribute that contains the raw permission data as a string, providing a convenient way to work with the underlying permission data.

With these tools, Databricks Administrators and Data Engineers can define their own `AclSupport` classes to support permissions for new types of objects, and use the `Permissions` dataclass to represent the permissions for a specific object type and ID. This provides a flexible and extensible solution for managing permissions in a Databricks workspace, improving the reliability and security of your Databricks environment.

1. MigratedGroup Class

The MigratedGroup class represents a group that has been migrated from one name to another. It stores information about the original and new names, members, external ID, and roles of the group. This class is useful for tracking changes to groups during the migration process and can be used to verify that all groups have been successfully migrated.

2. MigrationState Class

The MigrationState class holds the state of the migration process and provides methods for getting the target principal (i.e., the name of the group in the account) and temporary name (i.e., the renamed name of the group in the workspace) for a given group name. This class can be used to keep track of the progress of the migration process and to retrieve information about the new and old names of groups.

3. GroupMigrationStrategy Abstract Base Class

The GroupMigrationStrategy abstract base class defines the interface for a strategy that generates a list of MigratedGroup objects based on a mapping between workspace and account groups. This class allows for the creation of custom migration strategies that can be used to migrate groups based on specific rules or requirements.

4. MatchingNamesStrategy, MatchByExternalIdStrategy, RegexSubStrategy, and RegexMatchStrategy Classes

These classes are concrete implementations of the GroupMigrationStrategy interface. They provide different ways to generate a list of MigratedGroup objects based on a mapping between workspace and account groups. The MatchingNamesStrategy and MatchByExternalIdStrategy classes match groups based on their names and external IDs, respectively. The RegexSubStrategy and RegexMatchStrategy classes match groups using regular expressions for matching and substitution.

5. GroupManager Class

The GroupManager class is a subclass of CrawlerBase that manages groups in a Databricks workspace. It provides methods for renaming groups, reflecting account groups on the workspace, deleting original workspace groups, and validating group membership. The class also provides methods for listing workspace and account groups, getting group details, and deleting groups.

6. ConfigureGroups Class

The ConfigureGroups class provides a command-line interface for configuring the group migration process. It prompts the user to enter information about the group migration strategy, such as the renamed group prefix, regular expressions for matching and substitution, and a list of groups to migrate. The class also provides methods for validating user input and setting class variables based on the user's responses. This class simplifies the process of configuring the group migration process and allows for the easy specification of migration strategies.

Overall, this module provides a flexible and extensible framework for managing groups in a Databricks workspace, with support for different migration strategies and a command-line interface for configuring the migration process. This module enables Databricks Administrators and Data Engineers to efficiently manage groups and migrate them as needed, improving the overall management and security of the Databricks workspace.


**Module: Permission Manager for Databricks Workspace**

The Permission Manager module provides a robust and flexible solution for managing permissions for various objects in a Databricks workspace. It includes a `PermissionManager` class that enables administrators and data engineers to crawl, save, and apply permissions for clusters, tables, UDFs (User-Defined Functions), and secrets.

The `PermissionManager` class accepts a `backend` object, an `inventory_database` string, and a list of `AclSupport` objects as its input. The `AclSupport` objects define how to crawl, save, and apply permissions for specific types of objects in the workspace.

To use the module, you can create a `PermissionManager` instance by calling the `factory` method. This method sets up the necessary `AclSupport` objects for different types of objects in the workspace. Once the instance is created, you can call the `inventorize_permissions` method to crawl and save the permissions for all objects to the inventory database.

The `apply_group_permissions` method allows you to apply the permissions to a list of account groups, while the `verify_group_permissions` method verifies that the permissions are valid. This feature helps ensure that the right users have access to the right objects in your workspace and that their permissions are up-to-date and accurate.

Moreover, the Permission Manager module provides an extensible framework that enables users to define their own `AclSupport` objects and add them to the `PermissionManager` instance. This feature allows for customization and flexibility, making it easy to manage permissions for new objects or custom objects in the workspace.

Overall, the Permission Manager module is a powerful tool that helps administrators and data engineers manage permissions in a Databricks workspace efficiently and effectively. By providing a flexible and extensible framework, it enables users to tailor the solution to their specific needs and requirements.

----- END ----

Workspace Group Migration
---


# Dashboards permission migration


It provides a `RedashPermissionsSupport` class that supports listing, applying, and verifying permissions for objects in the workspace. 
The objects are managed using the `Listing` class, which is initialized with a function that returns a list of objects and an `ObjectTypePlural` enumeration value.

The `RedashPermissionsSupport` class has methods for getting crawler tasks, listing objects, and getting apply tasks. 
The `get_crawler_tasks` method returns a list of tasks that can be used to crawl the workspace for objects that need to have their permissions updated. The `object_types` method returns a set of object types that the `RedashPermissionsSupport` instance can manage. The `get_apply_task` method returns a partial function that can be used to apply updated permissions to a specific object in the workspace.
The `_safe_get_dbsql_permissions` method is a helper function that retrieves permissions for a specific object in the workspace. 
It is used by other methods in the class to ensure that permissions are retrieved safely and that any errors are handled appropriately.
The `_load_as_request` method is a helper function that takes a list of permissions and converts it into a list of `sql.AccessControl` objects, which can be used to update permissions.
The `_crawler_task` method is a task that can be used to crawl the workspace for objects that need to have their permissions updated. It takes an object ID and an `ObjectTypePlural` enumeration value as arguments and returns a `Permissions` object that contains the current permissions for the object.
The `_verify` method is a helper function that verifies that the current permissions for a specific object in the workspace match the expected permissions. It takes an `ObjectTypePlural` enumeration value, an object ID, and a list of `sql.AccessControl` objects as arguments.
The `get_verify_task` method returns a partial function that can be used to verify the permissions for a specific object in the workspace. It takes a `Permissions` object as an argument and returns a function that can be used to verify the permissions.
The `_applier_task` method is a task that can be used to apply updated permissions to a specific object in the workspace. It takes an `ObjectTypePlural` enumeration value, an object ID, and a list of `sql.AccessControl` objects as arguments.
The `_prepare_new_acl` method is a helper function that takes a list of `sql.AccessControl` objects and a `MigrationState` object and returns a new list of `sql.AccessControl` objects that include any necessary changes to the permissions.
The `redash_listing_wrapper` function is a decorator that can be used to convert a function that returns a list of objects into a function that returns an iterable of `SqlPermissionsInfo` objects. It takes a function and an `ObjectTypePlural` enumeration value as arguments and returns a new function that can be used to list objects.

# Entitlements migration

The code defines a ScimSupport class that implements the AclSupport interface for managing permissions in a workspace using SCIM (System for Cross-domain Identity Management) APIs.

The class initializes with a workspace client and an optional verify_timeout parameter to specify the timeout for permission verification. It also creates a snapshot of all the groups in the workspace with their display name, id, meta, roles, and entitlements.

The _is_item_relevant method checks if a permission item is relevant to the current migration state by checking if the item's object_id matches any of the group ids in the migration state.

The get_crawler_tasks method returns an iterator of partial functions for crawling the permissions of each group in the snapshot. It checks if the group has any roles or entitlements and returns a partial function to crawl the corresponding property.

The object_types method returns the set of object types supported by the ScimSupport class, which are 'roles' and 'entitlements'.

The get_apply_task method returns a partial function to apply the permissions of a permission item to a target group. It loads the values from the permission item, gets the target group from the snapshot using the group name in the account, and returns a partial function to apply the permissions using the _applier_task method.

The load_for_group method returns the set of roles and entitlements for a given group_id. It gets the group using the _safe_get_group method and loads the roles and entitlements if they exist.

The _crawler_task method creates a Permissions object for a group's roles or entitlements. It gets the corresponding property from the group and creates a Permissions object with the group id, object type, and the JSON string of the property values.

The _verify method verifies the permissions of a group by checking if the provided value matches the actual value of the property in the group. It gets the group using the _safe_get_group method and checks if the property exists and if all the values in the provided value match the actual values in the property.

The get_verify_task method returns a partial function to verify the permissions using the _verify method.

The _applier_task method applies the permissions of a permission item to a group using the SCIM patch API. It creates a list of patch operations and schemas for the provided value and property name and applies them to the group using the _safe_patch_group method.

The _safe_patch_group method applies the patch operations to a group using the workspace client's patch method. It handles PermissionDenied and NotFound exceptions and logs a warning message if either occurs.

The _safe_get_group method gets a group using the workspace client's get method. It handles PermissionDenied and NotFound exceptions and returns None if either occurs.

# Secret scope permissions

The `SecretScopesSupport` class is a concrete implementation of the `AclSupport` interface for managing permissions on secret scopes in a Databricks workspace. Here's an overview of the methods:

* `__init__`: Initializes the `SecretScopesSupport` object with a `WorkspaceClient` instance and an optional `verify_timeout` parameter.
* `get_crawler_tasks`: Returns an iterator of partial functions for crawling the ACLs of all secret scopes in the workspace. Each partial function takes no arguments and returns a `Permissions` object representing the ACLs of a single secret scope.
* `object_types`: Returns a set containing the string "secrets", indicating that this implementation deals with managing permissions on secret scopes.
* `get_apply_task`: Returns a partial function that can be used to apply the ACLs of a `Permissions` object to a target secret scope. The function first checks if the `Permissions` object is relevant to the current migration state, and if so, it transforms the ACLs to target the correct principals, then applies them using the `_applier_task` method.
* `_is_item_relevant`: A static method that checks if a `Permissions` object is relevant to the current migration state by checking if any of the principals in the ACLs are mentioned in the groups in the migration state.
* `secret_scope_permission`: Returns the current permission level for a given group on a given secret scope.
* `_reapply_on_failure`: A helper method for reapplying ACLs if the initial application fails. It checks if the ACLs have been applied correctly using the `_verify` method, and if not, it reapplies them.
* `_verify`: Verifies that the ACLs for a given secret scope and group are set to the expected permission level. If the ACLs are not set correctly, a `ValueError` is raised.
* `get_verify_task`: Returns a partial function that can be used to verify the ACLs of a `Permissions` object. The function takes a secret scope name and an iterator of `AclItem` objects as arguments, and checks that the ACLs are set correctly for each `AclItem`.
* `_applier_task`: Applies the ACLs of a single `Permissions` object to a target secret scope.

# Legacy table access control lists

This module provides a class `TableAclSupport` that implements the `AclSupport` interface for managing table ACL permissions in a Databricks workspace.
The `TableAclSupport` class takes in an instance of `GrantsCrawler` and `SqlBackend` classes, and a `verify_timeout` parameter in its constructor.
The `get_crawler_tasks` method returns an iterator of callable objects, each of which returns a `Permissions` object for a specific table ACL permission. The method first creates a dictionary of folded actions for each principal and object, where each value is a set of action types for that principal and object. It then iterates over the dictionary and creates a `Grant` object for each set of folded actions, and returns a callable object that returns a `Permissions` object for that `Grant`.
The `_from_reduced` method takes in the object type, object ID, principal, and action type, and returns a `Grant` object with those parameters.
The `object_types` method returns a set of object types that this `AclSupport` instance can manage.
The `get_apply_task` method takes in a `Permissions` object and a `MigrationState` object, and returns a callable object that applies the ACL permission in the `Permissions` object to the target principal in the `MigrationState` object. If the principal is not in the `MigrationState` object, the method returns `None`.
The `_apply_grant` method takes in a `Grant` object and applies the ACL permission to the target principal in the database.
The `_verify` method takes in the object type, object ID, and a `Grant` object, and returns `True` if the ACL permission in the `Grant` object matches the ACL permission for that object and principal in the database. If the ACL permission does not match, the method raises a `ValueError` with an error message.
The `get_verify_task` method takes in a `Permissions` object and returns a callable object that calls the `_verify` method with the object type, object ID, and `Grant` object from the `Permissions` object.



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

Appliers are expected to accept a list of `workspace_access.Permissions` and generate a list of callables that will
apply the
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
Make sure to install [databricks-sdk](https://docs.databricks.com/en/dev-tools/sdk-python.html) on the cluster to run
it.

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
from databricks.labs.ucx.workspace_access.groups import GroupManager
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

4. Create a migration state just for account groups

```python
from databricks.labs.ucx.workspace_access.manager import PermissionManager
from databricks.labs.ucx.workspace_access.groups import GroupMigrationState
from databricks.labs.ucx.framework.crawlers import StatementExecutionBackend

collected_groups = []
...

migration_state = GroupMigrationState()
for group in collected_groups:
    migration_state.add(None, None, group)

sql_backend = StatementExecutionBackend(ws, cfg.warehouse_id)

permission_manager = PermissionManager.factory(ws, sql_backend, cfg.inventory_database)
permission_manager.apply_group_permissions(migration_state, destination="account")
```

5. recovering permissions from a debug notebook with logs

```python
import logging
from logging.handlers import TimedRotatingFileHandler

databricks_logger = logging.getLogger("databricks")
databricks_logger.setLevel(logging.DEBUG)

ucx_logger = logging.getLogger("databricks.labs.ucx")
ucx_logger.setLevel(logging.DEBUG)

log_file = "/Workspace/Users/jaf2bh@example.com/recovery.log"

# files are available in the workspace only once their handlers are closed,
# so we rotate files log every 10 minutes.
#
# See https://docs.python.org/3/library/logging.handlers.html#logging.handlers.TimedRotatingFileHandler
file_handler = TimedRotatingFileHandler(log_file, when="M", interval=5)
log_format = "%(asctime)s %(levelname)s [%(name)s] {%(threadName)s} %(message)s"
log_formatter = logging.Formatter(fmt=log_format, datefmt="%H:%M:%S")
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.DEBUG)
databricks_logger.addHandler(file_handler)

sql_backend = StatementExecutionBackend(ws, cfg.warehouse_id)

try:
    permission_manager = PermissionManager.factory(ws, sql_backend, cfg.inventory_database)
    permission_manager.apply_group_permissions(migration_state, destination="account")
finally:
    # IMPORTANT!!!!
    file_handler.close()
```
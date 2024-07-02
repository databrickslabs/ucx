Workspace Group Migration
===

<!-- TOC -->
* [Workspace Group Migration](#workspace-group-migration)
* [Design](#design)
  * [Group Manager](#group-manager)
  * [Permission Manager](#permission-manager)
  * [ACL Support](#acl-support)
    * [Generic Permissions](#generic-permissions)
    * [Dashboard Permissions](#dashboard-permissions)
    * [Entitlements and Roles](#entitlements-and-roles)
    * [Secret Scope Permissions](#secret-scope-permissions)
    * [Legacy Table Access Controls](#legacy-table-access-controls)
* [Troubleshooting](#troubleshooting)
<!-- TOC -->

This feature introduces the ability to migrate groups from workspace level to account level in
the [group migration workflow](../README.md#group-migration-workflow). It helps you to upgrade all Databricks workspace assets:
Legacy Table ACLs, Entitlements, AWS instance profiles, Clusters, Cluster policies, Instance Pools,
Databricks SQL warehouses, Delta Live Tables, Jobs, MLflow experiments, MLflow registry, SQL Dashboards & Queries,
SQL Alerts, Token and Password usage permissions that are set on the workspace level, Secret scopes, Notebooks,
Directories, Repos, and Files.

It ensures that all the necessary groups are available in the workspace with the correct permissions, and removes any unnecessary groups and permissions.
The tasks in the group migration workflow depend on the output of the assessment workflow and can be executed in sequence to ensure a successful migration.
The output of each task is stored in Delta tables in the `$inventory_database` schema.

The group migration workflow can be executed multiple times to ensure that all the groups are migrated successfully and that all the necessary permissions are assigned.

1. `crawl_groups`: This task scans all groups for the local group migration scope.
2. `rename_workspace_local_groups`: This task renames workspace local groups by adding a `ucx-renamed-` prefix. This step is taken to avoid conflicts with account-level groups that may have the same name as workspace-local groups.
3. `reflect_account_groups_on_workspace`: This task adds matching account groups to this workspace. The matching account level group(s) must preexist(s) for this step to be successful. This step is necessary to ensure that the account-level groups are available in the workspace for assigning permissions.
4. `apply_permissions_to_account_groups`: This task assigns the full set of permissions of the original group to the account-level one. It covers local workspace-local permissions for all entities, including Legacy Table ACLs, Entitlements, AWS instance profiles, Clusters, Cluster policies, Instance Pools, Databricks SQL warehouses, Delta Live Tables, Jobs, MLflow experiments, MLflow registry, SQL Dashboards & Queries, SQL Alerts, Token and Password usage permissions, Secret Scopes, Notebooks, Directories, Repos, Files. This step is necessary to ensure that the account-level groups have the necessary permissions to manage the entities in the workspace.
5. `validate_groups_permissions`: This task validates that all the crawled permissions are applied correctly to the destination groups.
6. `delete_backup_groups`: This task removes all workspace-level backup groups, along with their permissions. This should only be executed after confirming that the workspace-local migration worked successfully for all the groups involved. This step is necessary to clean up the workspace and remove any unnecessary groups and permissions.

[[back to top](#workspace-group-migration)]

# Design

`MigratedGroup` class represents a group that has been migrated from one name to another and stores information about
the original and new names, as well as the group's members, external ID, and roles. The `MigrationState` class holds
the state of the migration process and provides methods for getting the target principal and temporary name for a given
group name.

[[back to top](#workspace-group-migration)]

## Group Manager

The `GroupManager` class is a `CrawlerBase` subclass that manages groups in a Databricks workspace. It provides methods
for renaming groups, reflecting account groups on the workspace, deleting original workspace groups, and validating
group membership. The class also provides methods for listing workspace and account groups, getting group details, and
deleting groups.

The `GroupMigrationStrategy` abstract base class defines the interface for a strategy that generates a list
of `MigratedGroup` objects based on a mapping between workspace and account groups.
The `MatchingNamesStrategy`, `MatchByExternalIdStrategy`, `RegexSubStrategy`, and `RegexMatchStrategy` classes are
concrete implementations of this interface. See [group name conflicts](group_name_conflict.md) for more details.

The `ConfigureGroups` class provides a command-line interface for configuring the group migration process during [installation](../README.md#installation).
It prompts the user to enter information about the group migration strategy, such as the renamed group prefix, regular expressions
for matching and substitution, and a list of groups to migrate. The class also provides methods for validating user input
and setting class variables based on the user's responses.

[[back to top](#workspace-group-migration)]

## Permission Manager

It enables to crawl, save, and apply permissions for [clusters](#generic-permissions),
[tables and UDFs (User-Defined Functions)](#legacy-table-access-controls), [secret scopes](#secret-scope-permissions),
[entitlements](#entitlements-and-roles), and [dashboards](#dashboard-permissions).

To use the module, you can create a `PermissionManager` instance by calling the `factory` method, which sets up
the necessary [`AclSupport` objects](#acl-support) for different types of objects in the workspace. Once the instance
is created, you can call the `inventorize_permissions` method to crawl and save the permissions for all objects to
the inventory database in the `permissions` table.

The `apply_group_permissions` method allows you to apply the permissions to a list of account groups, while
the [`verify_group_permissions` method](../README.md#validate-groups-membership-command) verifies that the permissions are valid.

[[back to top](#workspace-group-migration)]

## ACL Support

The `AclSupport` objects define how to crawl, save, and apply permissions for specific types of objects in the workspace:

* `get_crawler_tasks`: A method that returns a list of callables that crawl and return permissions for the objects supported by the `AclSupport` instance. This method should be implemented to provide the necessary logic for crawling permissions for the specific type of object supported.
* `get_apply_task`: A method that returns a callable that applies the permissions for a given `Permissions` object to a destination group, based on the group's migration state. The callable should not have any shared mutable state, ensuring thread safety and reducing the risk of bugs.
* `get_verify_task`: A method that returns a callable that verifies that the permissions for a given `Permissions` object are applied correctly to the destination group. This method can be used to ensure that permissions are applied as expected, helping to improve the reliability and security of your Databricks workspace.
* `object_types`: An abstract method that returns a set of strings representing the object types that the `AclSupport` instance supports. This method should be implemented to provide the necessary information about the object types supported by the `AclSupport` class.

The `Permissions` dataclass is used to represent the permissions for a specific object type and ID. The dataclass includes a `raw` attribute
that contains the raw permission data as a string, providing a convenient way to work with the underlying permission data.

[[back to top](#workspace-group-migration)]

### Generic Permissions

The `GenericPermissionsSupport` class is a concrete implementation of the [`AclSupport` interface](#acl-support) for
migrating permissions on various objects in a Databricks workspace. It is designed to be flexible and support almost any
object type in the workspace:

- clusters
- cluster policies
- instance pools
- sql warehouses
- jobs
- pipelines
- serving endpoints
- experiments
- registered models
- token usage
- password usage
- feature tables
- notebooks
- workspace folders

It takes in an instance of the `WorkspaceClient` class, a list of `Listing` objects, and a `verify_timeout` parameter in
its constructor. The `Listing` objects are responsible for listing the objects in the workspace, and
the `GenericPermissionsSupport` class uses these listings to crawl the ACL permissions for each object.

The `_apply_grant` method applies the ACL permission to the target principal in the database, and the `_verify` method
checks if the ACL permission in the `Grant` object matches the ACL permission for that object and principal in the database.
If the ACL permission does not match, the method raises a `ValueError` with an error message. The `get_verify_task` method
takes in a `Permissions` object and returns a callable object that calls the `_verify` method with the object type,
object ID, and `Grant` object from the `Permissions` object.

he `_safe_get_permissions` and `_safe_updatepermissions` methods are used to safely get and update the permissions for
a given object type and ID, respectively. These methods handle exceptions that may occur during the API calls and log
appropriate warning messages.

[[back to top](#workspace-group-migration)]

### Dashboard Permissions

Reflected in [RedashPermissionsSupport](../src/databricks/labs/ucx/workspace_access/redash.py). See [examples](../tests/integration/workspace_access/test_redash.py) for more details on how to use it as a library.

[[back to top](#workspace-group-migration)]

### Entitlements and Roles

The `ScimSupport` is [`AclSupport`](#acl-support) that creates a snapshot of all the groups in the workspace, including their display name, id, meta, roles, and entitlements.
The `_is_item_relevant` method checks if a permission item is relevant to the current migration state. The `get_crawler_tasks` method returns an iterator of partial functions
for crawling the permissions of each group in the snapshot. It checks if the group has any roles or entitlements and returns a partial function to crawl the corresponding property.

See [examples](../tests/integration/workspace_access/test_scim.py) for more details on how to use it as a library.

[[back to top](#workspace-group-migration)]

### Secret Scope Permissions

`SecretScopesSupport` is a concrete implementation of the [`AclSupport` interface](#acl-support) for crawling ACLs of
all secret scopes, applying and verifying ACLs, and checking if a `Permissions` object is relevant to the current
migration state. It simplifies the process of managing permissions on secret scopes by checking if the ACLs have been
applied correctly, and if not, automatically reapplying them.

[[back to top](#workspace-group-migration)]

### Legacy Table Access Controls

The `TableAclSupport` class is initialized with an instance of `GrantsCrawler` and `SqlBackend` classes, along with a `verify_timeout` parameter.
The class offers methods for crawling table ACL permissions, applying and verifying ACL permissions, and checking if a `Permissions` object is relevant to the current migration state.
The `get_crawler_tasks` method returns an iterator of callable objects, each of which returns a `Permissions` object for a specific table ACL permission.
The `_from_reduced` method creates a `Grant` object for each set of folded actions, and the `get_apply_task` method applies the ACL permission in the `Permissions` object to the target principal in the `MigrationState` object.
Furthermore, the `_apply_grant` method applies the ACL permission to the target principal in the database, while the `_verify` method checks if the ACL permission in
the `Grant` object matches the ACL permission for that object and principal in the database. The `get_verify_task` method calls the `_verify` method with the object type,
object ID, and `Grant` object from the `Permissions` object.

[[back to top](#workspace-group-migration)]

# Troubleshooting

Use [`DEBUG` notebook](../README.md#debug-notebook) to troubleshoot anything.

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

[[back to top](#workspace-group-migration)]

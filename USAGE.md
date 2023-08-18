# Group migration

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

## Permissions and entitlements that we inventorize

> Please note that inherited permissions will not be inventorized / migrated.
> We only cover direct permissions.

Group-level:

- [x] Entitlements (One of `workspace-access`, `databricks-sql-access`, `allow-cluster-create`, `allow-instance-pool-create`)
- [x] Roles (AWS Only, represents Instance Profile)

Compute infrastructure:

- [x] Clusters
- [x] Cluster policies
- [x] Pools
- [x] Databricks SQL warehouses

Workflows:

- [x] Delta Live Tables
- [x] Jobs

ML:

- [x] MLflow experiments
- [x] MLflow registry

SQL:

- [ ] Dashboard
- [ ] Queries
- [ ] Alerts

Security:

- [x] Tokens (token permissions are set on the workspace level. It basically says "this group can use tokens or not")
- [x] Passwords (only for AWS, it defines which groups can log in with passwords)
- [x] Secrets

Workspace:

- [x] Notebooks
- [x] Directories
- [x] Repos
- [x] Files

Data access:

- [ ] Table ACLS

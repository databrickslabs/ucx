# UC Migration Toolkit

This repo contains various functions and utilities for UC Upgrade.

## Latest working version and how-to

Please note that current project statis is 🏗️ **WIP**, but we have a minimal set of already working utilities.

To run the notebooks please use latest LTS Databricks Runtime (non-ML), without Photon, in a single-user cluster mode.

> If you have Table ACL Clusters or SQL Warehouse where ACL have been defined, you should create a TableACL cluster to
> run this notebook.

Please note that script is executed **only** on the driver node, therefore you'll need to use a Single Node Cluster with
sufficient amount of cores (e.g. 16 cores).

Recommended VM types are:

- Azure: `Standard_F16`
- AWS: `c4.4xlarge`
- GCP: `c2-standard-16`

**For now please switch to the `v0.0.2` tag in the GitHub to get the latest working version.**

**All instructions below are currently in WIP mode.**

## Group migration

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

## Development

This section describes setup and development process for the project.

### Local setup

- Install [hatch](https://github.com/pypa/hatch):

```shell
pip install hatch
```

- Create environment:

```shell
hatch env create
```

- Install dev dependencies:

```shell
hatch run pip install -e '.[dbconnect]'
```

- Pin your IDE to use the newly created virtual environment. You can get the python path with:

```shell
hatch run python -c "import sys; print(sys.executable)"
```

- You're good to go! 🎉

### Development process

Please note that you **don't** need to use `hatch` inside notebooks or in the Databricks workspace.
It's only introduced to simplify local development.

Write your code in the IDE. Please keep all relevant files under the `src/uc_migration_toolkit` directory.

Don't forget to test your code via:

```shell
hatch run test
```

Please note that all commits go through the CI process, and it verifies linting. You can run linting locally via:

```shell
hatch run lint:fmt
```



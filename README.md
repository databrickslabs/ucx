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

**For now please switch to the `v0.0.1` tag in the GitHub to get the latest working version.**

## Group migration

During the UC adoption, it's critical to move the groups (group objects) from the workspace to account level.

To deliver this migration, the following steps are performed:

- Local group objects are fetched from the workspace (or provided via configuration file)
- Groups are verified against the account level (existence, etc.)
- Workspace local group permissions are inventorized and saved into a Delta Table
- Backup groups are created on the workspace level
- Backup groups are entitled with permissions from the inventory table
- Workspace-level groups are deleted
- Account-level groups are created
- Account-level groups are entitled with permissions from the inventory table
- Backup groups are deleted
- Inventory table is cleaned up

## Permissions that we inventorize

Compute infrastructure:

- [ ] Clusters
- [ ] Cluster policies +
- [ ] Pools +
- [ ] Instance Profile (for AWS) +

Workflows:

- [ ] Delta Live Tables
- [ ] Jobs

ML:

- [ ] MLflow experiments
- [ ] MLflow registry
- [ ] Legacy Mlflow model endpoints (?)

SQL:

- [ ] Databricks SQL warehouses
- [ ] Dashboard
- [ ] Queries
- [ ] Alerts

Security:

- [ ] Tokens
- [ ] Passwords (for AWS)
- [ ] Secrets

Workspace:

- [ ] Notebooks in the Workspace FS
- [ ] Directories in the Workspace FS
- [ ] Files in the Workspace FS

Repos:

- [ ] User-level Repos
- [ ] Org-level Repos

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
hatch run pip install -e '.[dev]'
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



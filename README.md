# ![UCX by Databricks Labs](docs/logo-no-background.png)

Your best companion for upgrading to Unity Catalog. It helps you to upgrade all Databricks workspace assets:
Legacy Table ACLs, Entitlements, AWS instance profiles, Clusters, Cluster policies, Instance Pools, Databricks SQL warehouses, Delta Live Tables, Jobs, MLflow experiments, MLflow registry, SQL Dashboards & Queries, SQL Alerts, Token and Password usage permissions that are set on the workspace level, Secret scopes, Notebooks, Directories, Repos, Files.

[![build](https://github.com/databrickslabs/ucx/actions/workflows/push.yml/badge.svg)](https://github.com/databrickslabs/ucx/actions/workflows/push.yml) [![codecov](https://codecov.io/github/databrickslabs/ucx/graph/badge.svg?token=p0WKAfW5HQ)](https://codecov.io/github/databrickslabs/ucx)

See [contributing instructions](CONTRIBUTING.md) to help improve this project.


## Introduction
UCX will guide you, the Databricks customer, through the process of upgrading your account, groups, workspaces, jobs etc. to Unity Catalog.

1. The upgrade process will first install code, libraries, and workflows into your workspace.
2. After installation, you will run a series of workflows and examine the output.

UCX leverages Databricks Lakehouse platform to upgrade itself. The upgrade process includes creating jobs, notebooks, and deploying code and configuration files. 

By running the installation you install the assessment job and several upgrade jobs. The assessment and upgrade jobs are outlined in the custom-generated README.py that is created by the installer.

The custom-generated `README.py`, `config.yaml`, and other assets are placed into your Databricks workspace home folder, into a sub-folder named `.ucx`. See [interactive tutorial](https://app.getreprise.com/launch/zXPxBZX/).


Once the custom Databricks jobs are installed, begin by triggering the assessment job. The assessment job can be found under your workflows or via the active link in the README.py. Once the assessment job is complete, you can review the results in the custom-generated Databricks dashboard (linked to by the custom README.py found in the workspace folder created for you).


You will need an account, unity catalog, and workspace administrative authority to complete the upgrade process. To run the installer, you will need to set up `databricks-cli` and a credential, [following these instructions.](https://docs.databricks.com/en/dev-tools/cli/databricks-cli.html) Additionally, the interim metadata and config data being processed by UCX will be stored into a Hive Metastore database schema generated at install time.


For questions, troubleshooting or bug fixes, please see your Databricks account team or submit an issue to the [Databricks UCX GitHub repo](https://github.com/databrickslabs/ucx)

## Installation
### Prerequisites
1. Get trained on UC [[free instructor-led training 2x week]](https://customer-academy.databricks.com/learn/course/1683/data-governance-with-unity-catalog?generated_by=302876&hash=4eab6668f83636ba44d109880002b293e8dda6dd) [[full training schedule]](https://files.training.databricks.com/static/ilt-sessions/half-day-workshops/index.html)
2. You will need a desktop computer, running Windows, macOS, or Linux; This computer is used to install the UCX toolkit onto the Databricks workspace, the computer will also need:
    -  Network access to your Databricks Workspace
    -  Network access to the Internet to retrieve additional Python packages (e.g. PyYAML, databricks-sdk,...) and access https://github.com
    -  Python 3.10 or later - [Windows instructions](https://www.python.org/downloads/)
    -  Databricks CLI with a workspace [configuration profile](https://docs.databricks.com/en/dev-tools/auth.html#databricks-client-unified-authentication) for workspace - [instructions](https://docs.databricks.com/en/dev-tools/cli/install.html)
    -  Your Windows computer will need a shell environment (GitBash or ([WSL](https://learn.microsoft.com/en-us/windows/wsl/about))
3. Within the Databricks Workspace you will need:
    - Workspace administrator access permissions
    - The ability for the installer to upload Python Wheel files to DBFS and Workspace FileSystem
    - A PRO or Serverless SQL Warehouse
    - The Assessment workflow will create a legacy "No Isolation Shared" and a legacy "Table ACL" jobs clusters needed to inventory Hive Metastore Table ACLS
    - If your Databricks Workspace relies on an external Hive Metastore (such as AWS Glue), make sure to read the [External HMS Document](docs/external_hms_glue.md).
4. [[AWS]](https://docs.databricks.com/en/administration-guide/users-groups/best-practices.html) [[Azure]](https://learn.microsoft.com/en-us/azure/databricks/administration-guide/users-groups/best-practices)] [[GCP]](https://docs.gcp.databricks.com/administration-guide/users-groups/best-practices.html) Account level Identity Setup
5. [[AWS]](https://docs.databricks.com/en/data-governance/unity-catalog/create-metastore.html) [[Azure]](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/create-metastore) [[GCP]](https://docs.gcp.databricks.com/data-governance/unity-catalog/create-metastore.html) Unity Catalog Metastore Created (per region)

#### Installing Databricks CLI on macOS
![macos_install_databricks](docs/macos_1_databrickslabsmac_installdatabricks.gif)

#### Install Databricks CLI via curl on Windows
![winos_install_databricks](docs/winos_1_databrickslabsmac_installdatabricks.gif)

### Download & Install

We only support installations and upgrades through [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html), as UCX requires an installation script run to make sure all the necessary and correct configurations are in place.

#### Install UCX
Install UCX via Databricks CLI:
```commandline
databricks labs install ucx
```

This will start an interactive installer with a number of configuration questions:
- Select a workspace profile that has been defined in `~/.databrickscfg`
- Provide the name of the inventory database where UCX will store the assessment results. This will be in the workspace `hive_metastore`. Defaults to `ucx`
- Create a new or select an existing SQL warehouse to run assessment dashboards on. The existing warehouse must be Pro or Serverless.
- Configurations for workspace local groups migration:
  - Provide a backup prefix. This is used to rename workspace local groups after they have been migrated. Defaults to `db-temp-`
  - Select a workspace local groups migration strategy. UCX offers matching by name or external ID, using a prefix/suffix, or using regex.
  - Provide a specific list of workspace local groups (or all groups) to be migrated.
- Select a Python log level, e.g. `DEBUG`, `INFO`. Defaults to `INFO`
- Provide the level of parallelism, which limit the number of concurrent operation as UCX scans the workspace. Defaults to 8.
- Select whether UCX should connect to the external HMS, if a cluster policy with external HMS is detected. Defaults to no.

After this, UCX will be installed locally and a number of assets will be deployed in the selected workspace. These assets are available under the installation folder, i.e. `/Users/<your user>/.ucx/`

![macos_install_ucx](docs/macos_2_databrickslabsmac_installucx.gif)

#### Upgrade UCX
Verify that UCX is installed
```text
databricks labs installed

Name  Description                            Version
ucx   Unity Catalog Migration Toolkit (UCX)  <version>
```
Upgrade UCX via Databricks CLI:
```commandline
databricks labs upgrade ucx
```
The prompts will be similar to [Installation](#install-ucx)

![macos_upgrade_ucx](docs/macos_3_databrickslabsmac_upgradeucx.gif)

#### Uninstall UCX
Uninstall UCX via Databricks CLI:
```commandline
databricks labs uninstall ucx
```

Databricks CLI will confirm a few options:
- Whether you want to remove all ucx artefacts from the workspace as well. Defaults to no.
- Whether you want to delete the inventory database in `hive_metastore`. Defaults to no.

![macos_uninstall_ucx](docs/macos_4_databrickslabsmac_uninstallucx.gif)

### Using UCX

After installation, a number of UCX workflows will be available in the workspace. `<installation_path>/README` contains further instructions and explanations of these workflows.
UCX also provides a number of command line utilities accessible via `databricks labs ucx`.

#### Understanding assessment report

After UCX assessment workflow is executed, the assessment dashboard will be populated with findings and common recommendations.
[This guide](docs/assessment.md) talks about them in more details.

#### Synchronising workspace info
Use to upload workspace config to all workspaces in the account where UCX is installed. UCX will prompt you to select an account profile that has been defined in `~/.databrickscfg`

```commandline
databricks labs ucx sync-workspace-info
```

#### Saving AWS instance profiles
Use to identify all instance profiles in the workspace, and map their access to S3 buckets. This requires `awscli` to be installed and configured.

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=databrickslabs/ucx&type=Date)](https://star-history.com/#databrickslabs/ucx)

## Project Support
Please note that all projects in the databrickslabs GitHub account are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs).  They are provided AS-IS, and we do not make any guarantees of any kind.  Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo.  They will be reviewed as time permits, but there are no formal SLAs for support.

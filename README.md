![UCX by Databricks Labs](docs/logo-no-background.png)

Your best companion for upgrading to Unity Catalog. It helps you to upgrade all Databricks workspace assets:
Legacy Table ACLs, Entitlements, AWS instance profiles, Clusters, Cluster policies, Instance Pools, Databricks SQL warehouses, Delta Live Tables, Jobs, MLflow experiments, MLflow registry, SQL Dashboards & Queries, SQL Alerts, Token and Password usage permissions that are set on the workspace level, Secret scopes, Notebooks, Directories, Repos, Files.

[![build](https://github.com/databrickslabs/ucx/actions/workflows/push.yml/badge.svg)](https://github.com/databrickslabs/ucx/actions/workflows/push.yml) [![codecov](https://codecov.io/github/databrickslabs/ucx/graph/badge.svg?token=p0WKAfW5HQ)](https://codecov.io/github/databrickslabs/ucx)

See [contributing instructions](CONTRIBUTING.md) to help improve this project.

<!-- TOC -->
  * [Introduction](#introduction)
  * [Installation](#installation)
    * [Prerequisites](#prerequisites)
      * [Install Databricks CLI on macOS](#install-databricks-cli-on-macos)
      * [Install Databricks CLI via curl on Windows](#install-databricks-cli-via-curl-on-windows)
    * [Download & Install](#download--install)
      * [Install UCX](#install-ucx)
      * [Upgrade UCX](#upgrade-ucx)
      * [Uninstall UCX](#uninstall-ucx)
  * [Using UCX](#using-ucx)
    * [Executing assessment job](#executing-assessment-job)
    * [Understanding assessment report](#understanding-assessment-report)
    * [Scanning for legacy credentials and mapping access](#scanning-for-legacy-credentials-and-mapping-access)
      * [AWS](#aws)
      * [Azure](#azure)
    * [Producing table mapping](#producing-table-mapping)
    * [Synchronising UCX configurations](#synchronising-ucx-configurations)
    * [Validating group membership](#validating-group-membership)
  * [Star History](#star-history)
  * [Project Support](#project-support)
<!-- TOC -->

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
4. A number of commands also require Databricks account administrator access permissions, e.g. `sync-workspace-info`
5. [[AWS]](https://docs.databricks.com/en/administration-guide/users-groups/best-practices.html) [[Azure]](https://learn.microsoft.com/en-us/azure/databricks/administration-guide/users-groups/best-practices)] [[GCP]](https://docs.gcp.databricks.com/administration-guide/users-groups/best-practices.html) Account level Identity Setup
6. [[AWS]](https://docs.databricks.com/en/data-governance/unity-catalog/create-metastore.html) [[Azure]](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/create-metastore) [[GCP]](https://docs.gcp.databricks.com/data-governance/unity-catalog/create-metastore.html) Unity Catalog Metastore Created (per region)

#### Install Databricks CLI on macOS
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
  - Select a workspace local groups migration strategy. UCX offers matching by name or external ID, using a prefix/suffix, or using regex. See [this](docs/group_name_conflict.md) for more details
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

## Using UCX

After installation, a number of UCX workflows will be available in the workspace. `<installation_path>/README` contains further instructions and explanations of these workflows.
UCX also provides a number of command line utilities accessible via `databricks labs ucx`.

### Executing assessment job
The assessment workflow can be triggered using the Databricks UI, or via the command line
```commandline
databricks labs ucx ensure-assessment-run
```
![ucx_assessment_workflow](docs/ucx_assessment_workflow.png)

### Understanding assessment report

After UCX assessment workflow is executed, the assessment dashboard will be populated with findings and common recommendations.
[This guide](docs/assessment.md) talks about them in more details.

### Scanning for legacy credentials and mapping access
#### AWS
Use to identify all instance profiles in the workspace, and map their access to S3 buckets. 
This requires `awscli` to be installed and configured.

```commandline
databricks labs ucx save-aws-iam-profiles
```

#### Azure
Use to identify all storage account used by tables, identify the relevant Azure service principals and their permissions on each storage account.
This requires `azure-cli` to be installed and configured. 

```commandline
databricks labs ucx save-azure-storage-accounts
```

### Producing table mapping
Use to create a table mapping CSV file, which provides the target mapping for all `hive_metastore` tables identified by the assessment workflow.
This file can be reviewed offline and later will be used for table migration.

```commandline
databricks labs ucx table-mapping 
```

### Managing cross-workspace installation
When installing UCX across multiple workspaces, users needs to keep UCX configurations in sync. The below commands address that.

**Recommended:** An account administrator executes `sync-workspace-info` to upload the current UCX workspace configurations to all workspaces in the account where UCX is installed. 
UCX will prompt you to select an account profile that has been defined in `~/.databrickscfg`.

```commandline
databricks labs ucx sync-workspace-info
```

**Not recommended:** If an account admin is not available to execute `sync-workspace-info`, workspace admins can manually upload the current ucx workspace config to specific target workspaces.
UCX will ask to confirm the current workspace name, and the ID & name of the target workspaces

```commandline
databricks labs ucx manual-workspace-info
```

### Validating group membership
Use to validate workspace-level & account-level groups to identify any discrepancies in membership after migration.

```commandline
databricks labs ucx validate-groups-membership
```

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=databrickslabs/ucx&type=Date)](https://star-history.com/#databrickslabs/ucx)

## Project Support
Please note that all projects in the databrickslabs GitHub account are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs).  They are provided AS-IS, and we do not make any guarantees of any kind.  Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo.  They will be reviewed as time permits, but there are no formal SLAs for support.

# UCX - Unity Catalog Migration Toolkit

[![build](https://github.com/databrickslabs/ucx/actions/workflows/push.yml/badge.svg)](https://github.com/databrickslabs/ucx/actions/workflows/push.yml) [![codecov](https://codecov.io/github/databrickslabs/ucx/graph/badge.svg?token=p0WKAfW5HQ)](https://codecov.io/github/databrickslabs/ucx)

Your best companion for enabling the Unity Catalog. It helps you to migrate all Databricks workspace assets:
Entitlements, AWS instance profiles, Clusters, Cluster policies, Instance Pools, Databricks SQL warehouses, Delta Live 
Tables, Jobs, MLflow experiments, MLflow registry, SQL Dashboards & Queries, SQL Alerts, Token and Password usage 
permissions that are set on the workspace level, Secret scopes, Notebooks, Directories, Repos, Files.

See [contributing instructions](CONTRIBUTING.md) to help improve this project.

## Introduction
UCX will guide you, the Databricks customer through the process of upgrading your account, groups, workspaces, jobs etc to Unity Catalog. 

The Unity Catalog upgrade process starts with an `install.sh` which creates a custom installation. By running the installation you install the assessment job and a number of upgrade jobs. The assessment and upgrade jobs are outlined in the custom generated README that is created by the installer. Once the custom Databricks jobs are installed, begin by triggering the assessment job. Once the assessment job is complete, you can review the results in the custom generated Databricks dashboard (linked to by the custom README.py found in the workspace folder created for you).

You will need account, unity catalog and workspace administrative authority to complete the upgrade process.

## Installation

As a customer, download the latest release from github onto your laptop/desktop machine. Unzip or untar the release.

The `./install.sh` script will guide you through installation process. 
Make sure you have Python 3.10 (or greater) 
installed on your workstation, and you've configured authentication for 
the [Databricks Workspace](https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#default-authentication-flow).

![install wizard](./examples/ucx-install.gif)

The easiest way to install and authenticate is through a [Databricks configuration profile](https://docs.databricks.com/en/dev-tools/auth.html#databricks-client-unified-authentication):

```shell
export DATABRICKS_CONFIG_PROFILE=ABC
./install.sh
```

You can also specify environment variables in a more direct way, like in this example for installing 
on an Azure Databricks Workspace using the Azure CLI authentication:

```shell
az login
export DATABRICKS_HOST=https://adb-123....azuredatabricks.net/
./install.sh
```

Please follow the instructions in `./install.sh`, which will open a notebook with the description of all jobs to trigger. The journey starts with assessment. 

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=databrickslabs/ucx&type=Date)](https://star-history.com/#databrickslabs/ucx)

## Project Support
Please note that all projects in the /databrickslabs github account are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs).  They are provided AS-IS and we do not make any guarantees of any kind.  Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo.  They will be reviewed as time permits, but there are no formal SLAs for support.

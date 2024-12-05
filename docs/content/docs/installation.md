---
linkTitle: "Installation"
title: Installation
---

This guide will help you to check the prerequisites and install UCX.

## Overview

Briefly, the process involves the following steps:

{{% steps %}}

### Setting up the environment

Please fullfill the [prerequisites](#prerequisites) before installing UCX.

### Install and authenticate Databricks CLI

Please [install and authenticate Databricks CLI](#install-and-authenticate-databricks-cli) on your local machine.

### Install the toolkit 

Install UCX via Databricks CLI. Please follow the instructions to [install UCX](#install-ucx).

### [Optional] Joining a collection

Workspaces can be grouped into collections to manage UCX migration at collection level.
Please follow the instructions to [join a collection](#joining-a-collection) if you want to join an existing collection or create a new one.

{{% /steps %}}



## Prerequisites

{{< callout type="info" >}}
To install UCX, user must have Databricks Workspace Administrator privileges.
{{< /callout >}}

{{< callout type="warning" >}}
Running UCX as a Service Principal is not supported.
{{< /callout >}}

### Local machine
- Databricks CLI v0.213 or later. See [instructions](#authenticate-databricks-cli).
- Python 3.10 or later. See [Windows](https://www.python.org/downloads/windows/) instructions.


### Network access from local machine
- Network access to your Databricks Workspace used for the [installation process](#install-ucx).
- Network access to the Internet for [pypi.org](https://pypi.org) and [github.com](https://github.com) from machine running the installation.

### Account-level settings
- Account level Identity Setup. See instructions for [AWS](https://docs.databricks.com/en/administration-guide/users-groups/best-practices.html), [Azure](https://learn.microsoft.com/en-us/azure/databricks/administration-guide/users-groups/best-practices), and [GCP](https://docs.gcp.databricks.com/administration-guide/users-groups/best-practices.html).
- Unity Catalog Metastore Created (per region). See instructions for [AWS](https://docs.databricks.com/en/data-governance/unity-catalog/create-metastore.html), [Azure](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/create-metastore), and [GCP](https://docs.gcp.databricks.com/data-governance/unity-catalog/create-metastore.html).


### Workspace-level settings
- Workspace should have Premium or Enterprise tier.
- A PRO or Serverless SQL Warehouse to render the [report](docs/assessment.md) for the [assessment workflow](#assessment-workflow).

{{< callout type="info" >}}
If your Databricks Workspace relies on an external Hive Metastore (such as AWS Glue), make sure to read [this guide](docs/external_hms_glue.md)
{{< /callout >}}


## Install and authenticate Databricks CLI

{{< callout type="info" >}}
We only support installations and upgrades through [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html), as UCX requires an installation script run to make sure all the necessary and correct configurations are in place. 
{{< /callout >}}

Please follow the instructions below to install Databricks CLI on your local machine:

{{< tabs items="macOS,Windows" >}}
    {{< tab >}}Please follow this process: ![macos_install_databricks](/images/macos_1_databrickslabsmac_installdatabricks.gif)
    {{< /tab >}}

    {{< tab >}}Please follow this process: ![windows_install_databricks.png](/images/windows_install_databricks.png)
    {{< /tab >}}

{{< /tabs >}}


Once you install Databricks CLI, authenticate your current machine to a Databricks Workspace:

```bash
databricks auth login --host WORKSPACE_HOST
```

To enable debug logs, simply add `--debug` flag to any command.

## Install UCX

Install UCX via Databricks CLI:

```bash
databricks labs install ucx
```

You'll be prompted to select a [configuration profile](https://docs.databricks.com/en/dev-tools/auth.html#databricks-client-unified-authentication) created by `databricks auth login` command.

After running this command, UCX will be installed locally and a number of assets will be deployed in the selected workspace.

Upon the first installation, you're prompted for a workspace local [group migration strategy](docs/group_name_conflict.md).
Based on user input, the class creates a new cluster policy with the specified configuration. The user can review and confirm the configuration, which is saved to the workspace and can be opened in a web browser.

These assets are available under the installation folder, i.e. `/Applications/ucx` is the default installation folder. Please check [here](#advanced-force-install-over-existing-ucx) for more details.

{{< callout type="info" >}}
You can also install a specific version by specifying it like `@vX.Y.Z`:
```bash
databricks labs install ucx@vX.Y.Z
```
{{< /callout >}}

Here is a demo of the installation process:
![macos_install_ucx](/images/macos_2_databrickslabsmac_installucx.gif)


## Joining a collection

At the end of the installation, the user will be prompted if the current installation needs to join an existing collection (or create new collection if none present).

For large organization with many workspaces, grouping workspaces into collection helps in managing UCX migration at collection level (instead of workspaces level) 

{{< callout type="warning" >}}
User should be an account admin to be able to join a collection.
{{< /callout >}}



## Implementation details

The `WorkspaceInstaller` class is used to create a new configuration for Unity Catalog migration in a Databricks workspace.

It guides the user through a series of prompts to gather necessary information, such as:
- selecting an inventory database
- choosing a PRO or SERVERLESS SQL warehouse
- specifying a log level and number of threads
- setting up an external Hive Metastore (if necessary)
  


The [`WorkspaceInstallation`](src/databricks/labs/ucx/install.py) manages the installation and uninstallation of UCX in a workspace. 

It handles the configuration and exception management during the installation process. 
The installation process creates dashboards, databases, and jobs.

It also includes the creation of a database with given configuration and the deployment of workflows with specific settings. The installation process can handle exceptions and infer errors from job runs and task runs. 

The workspace installation uploads wheels, creates cluster policies,and wheel runners to the workspace. 

It can also handle the creation of job tasks for a given task, such as job dashboard tasks, job notebook tasks, and job wheel tasks. The class handles the installation of UCX, including configuring the workspace, installing necessary libraries, and verifying the installation, making it easier for users to migrate their workspaces to UCX.



## Installation resources

The following resources are installed by UCX:

| Installed UCX resources                           | Description                                                                                      |
|---------------------------------------------------|--------------------------------------------------------------------------------------------------|
| [Inventory database](./docs/table_persistence.md) | A Hive metastore database/schema in which UCX persist inventory required for the upgrade process |
| [Workflows](#workflows)                           | Workflows to execute UCX                                                                         |
| [Dashboards](#dashboards)                         | Dashboards to visualize UCX outcomes                                                             |
| [Installation folder](#installation-folder)       | A workspace folder containing UCX files in `/Applications/ucx/`.                                 |

## Installation folder

UCX is in installed in the workspace folder `/Applications/ucx/`. This folder contains UCX's code resources, like the
[source code](./src) from this GitHub repository and the [dashboard](#dashboards). Generally, these resources are not
*directly* used by UCX users. Resources that can be of importance to users are detailed in the subsections below.

### Readme notebook

![readme](/images/readme-notebook.png)

Every installation creates a `README` notebook with a detailed description of all deployed workflows and their tasks,
providing quick links to the relevant workflows and dashboards.



### Debug notebook

![debug](/images/debug-notebook.png)

Every installation creates a `DEBUG` notebook, that initializes UCX as a library for you to execute interactively.



### Debug logs

![debug](/images/debug-logs.png)

The [workflow](#workflows) runs store debug logs in the `logs` folder of the installation folder. The logs are flushed
every minute in a separate file. Debug logs for [the command-line interface](#authenticate-databricks-cli) are shown
by adding the `--debug` flag:

```bash
databricks --debug labs ucx <command>
```



### Installation configuration

In the installation folder, the UCX configuration is kept.

## Advanced installation options

Advanced installation options are detailed below.

### Force install over existing UCX
Using an environment variable `UCX_FORCE_INSTALL` you can force the installation of UCX over an existing installation.
The values for the environment variable are 'global' and 'user'.

Global Install: When UCX is installed at '/Applications/ucx'
User Install: When UCX is installed at '/Users/<user>/.ucx'

If there is an existing global installation of UCX, you can force a user installation of UCX over the existing installation by setting the environment variable `UCX_FORCE_INSTALL` to 'global'.

At this moment there is no global override over a user installation of UCX. As this requires migration and can break existing installations.


| global | user | expected install location | install_folder      | mode                                            |
|--------|------|---------------------------|---------------------|-------------------------------------------------|
| no     | no   | default                   | `/Applications/ucx` | install                                         |
| yes    | no   | default                   | `/Applications/ucx` | upgrade                                         |
| no     | yes  | default                   | `/Users/X/.ucx`     | upgrade (existing installations must not break) |
| yes    | yes  | default                   | `/Users/X/.ucx`     | upgrade                                         |
| yes    | no   | **USER**                  | `/Users/X/.ucx`     | install (show prompt)                           |
| no     | yes  | **GLOBAL**                | ...                 | migrate                                         |


* `UCX_FORCE_INSTALL=user databricks labs install ucx` - will force the installation to be for user only
* `UCX_FORCE_INSTALL=global databricks labs install ucx` - will force the installation to be for root only




### Installing UCX on all workspaces within a Databricks account
Setting the environment variable `UCX_FORCE_INSTALL` to 'account' will install UCX on all workspaces within a Databricks account.

* `UCX_FORCE_INSTALL=account databricks labs install ucx`

After the first installation, UCX will prompt the user to confirm whether to install UCX on the remaining workspaces with the same answers. If confirmed, the remaining installations will be completed silently.

This installation mode will automatically select the following options:
* Automatically create and enable HMS lineage init script
* Automatically create a new SQL warehouse for UCX assessment



### Installing UCX with company hosted pypi mirror
{{< callout type="info" >}}
If you're using custom Pypi, during installation, please reply *yes* to the question "Does the given workspace block Internet access"?
{{< /callout >}}

Some enterprise block the public pypi index and host a company controlled pypi mirror. 

To install UCX while using a company hosted pypi mirror for finding its dependencies, add all UCX dependencies to the company hosted pypi mirror (see
"dependencies" in [`pyproject.toml`](./pyproject.toml)) and set the environment variable `PIP_INDEX_URL` to the company hosted PYPI mirror URL while installing UCX:

```bash
PIP_INDEX_URL="https://url-to-company-hosted-pypi.internal" databricks labs install ucx
```





## Upgrading UCX for newer versions

Verify that UCX is installed

```bash
databricks labs installed

Name  Description                            Version
ucx   Unity Catalog Migration Toolkit (UCX)  <version>
```

Upgrade UCX via Databricks CLI:

```bash
databricks labs upgrade ucx
```

The prompts will be similar to [Installation](#install-ucx)

![macos_upgrade_ucx](/images/macos_3_databrickslabsmac_upgradeucx.gif)



## Uninstall UCX

Uninstall UCX via Databricks CLI:

```bash
databricks labs uninstall ucx
```

Databricks CLI will confirm a few options:
- Whether you want to remove all ucx artefacts from the workspace as well. Defaults to no.
- Whether you want to delete the inventory database in `hive_metastore`. Defaults to no.

![macos_uninstall_ucx](/images/macos_4_databrickslabsmac_uninstallucx.gif)




## Next

Use the assessment workflow as described here:

{{< cards >}}
  {{< card link="../tutorial" title="Tutorial" icon="document-text" subtitle="Learn how to use UCX" >}}
{{< /cards >}}
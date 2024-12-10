---
title: Advanced setup steps
linkTitle: Advanced setup steps
weight: 5
---

Advanced installation options and other actions are detailed below.

## Force install over existing UCX
Using an environment variable `UCX_FORCE_INSTALL` you can force the installation of UCX over an existing installation.

The values for the environment variable are `global` and `user`.

- Global Install: When UCX is installed at `/Applications/ucx`
- User Install: When UCX is installed at `/Users/<user>/.ucx`

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




## Installing UCX on all workspaces within a Databricks account
Setting the environment variable `UCX_FORCE_INSTALL` to 'account' will install UCX on all workspaces within a Databricks account.

* `UCX_FORCE_INSTALL=account databricks labs install ucx`

After the first installation, UCX will prompt the user to confirm whether to install UCX on the remaining workspaces with the same answers. If confirmed, the remaining installations will be completed silently.

This installation mode will automatically select the following options:
* Automatically create and enable HMS lineage init script
* Automatically create a new SQL warehouse for UCX assessment



## Installing UCX with company hosted pypi mirror
{{< callout type="info" >}}
If you're using custom Pypi, please reply *yes* to the question "Does the given workspace block Internet access"?
{{< /callout >}}

Some enterprise block the public pypi index and host a company controlled pypi mirror. 

To install UCX while using a company hosted pypi mirror for finding its dependencies, add all UCX dependencies to the company hosted pypi mirror (see
"dependencies" in [`pyproject.toml`](https://github.com/databrickslabs/ucx/blob/main/pyproject.toml)) and set the environment variable `PIP_INDEX_URL` to the company hosted PYPI mirror URL while installing UCX:

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

The prompts will be similar to [Installation](docs/installation/install_ucx)

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
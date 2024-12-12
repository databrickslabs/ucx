---
linkTitle: "Install UCX"
title: Install UCX
weight: 3
---


Install UCX via Databricks CLI:

```bash
databricks labs install ucx
```

You'll be prompted to select a [configuration profile](https://docs.databricks.com/en/dev-tools/auth.html#databricks-client-unified-authentication) created by `databricks auth login` command.

After running this command, UCX will be installed locally and a number of assets will be deployed in the selected workspace.

These assets are available under the installation folder, i.e. `/Applications/ucx` is the default installation folder. Please check [here](docs/installation/advanced.md#force-install-over-existing-ucx) for more details.

{{< callout type="info" >}}
You can also install a specific version by specifying it like `@vX.Y.Z`:
```bash
databricks labs install ucx@vX.Y.Z
```
{{< /callout >}}

Here is a demo of the installation process:
![macos_install_ucx](/images/macos_2_databrickslabsmac_installucx.gif)


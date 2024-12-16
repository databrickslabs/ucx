---
linkTitle: "Install and authenticate Databricks CLI"
title: "Install and authenticate Databricks CLI"
weight: 2
---


{{< callout type="info" >}}
We only support installations and upgrades through [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html), as UCX requires an installation script run to make sure all the necessary and correct configurations are in place. 
{{< /callout >}}

Please follow the instructions below to install Databricks CLI on your local machine:

{{< tabs items="macOS,Windows" >}}
    {{< tab >}}
    For MacOS we recommend using `brew`:
    ![macos_install_databricks](/images/macos_1_databrickslabsmac_installdatabricks.gif)
    {{< /tab >}}

    {{< tab >}}
    For Windows we recommend using `winget`:
    ![windows_install_databricks.png](/images/windows_install_databricks.png)
    {{< /tab >}}

{{< /tabs >}}


Once you install Databricks CLI, authenticate your current machine to a Databricks Workspace:

```bash
databricks auth login --host WORKSPACE_HOST
```

To enable debug logs, simply add `--debug` flag to any command.
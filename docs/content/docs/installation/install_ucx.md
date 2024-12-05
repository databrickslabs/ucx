
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


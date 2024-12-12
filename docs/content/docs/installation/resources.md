## Installation resources

The following resources are installed by UCX:

| Installed UCX resources                           | Description                                                                                      |
|---------------------------------------------------|--------------------------------------------------------------------------------------------------|
| [Inventory database](docs/reference/table_persistence.md) | A Hive metastore database/schema in which UCX persist inventory required for the upgrade process |
| [Workflows](docs/reference/workflows/)                           | Workflows to execute UCX                                                                         |
| [Dashboards](docs/reference/dashboards.md)                         | Dashboards to visualize UCX outcomes                                                             |
| [Installation folder](#installation-folder)       | A workspace folder containing UCX files in `/Applications/ucx/`.                                 |

## Installation folder

UCX is in installed in the workspace folder `/Applications/ucx/`. This folder contains UCX's code resources, like the
[source code](https://github.com/databrickslabs/ucx/tree/main/src/databricks) from UCX GitHub repository and the [dashboard](docs/reference/dashboards). Generally, these resources are not
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

The [workflow](docs/reference/workflows.md) runs store debug logs in the `logs` folder of the installation folder. The logs are flushed every minute in a separate file. Debug logs for [the command-line interface](docs/installation/databricks_cli.md) are shown by adding the `--debug` flag:

```bash
databricks --debug labs ucx <command>
```

## Installation configuration persistence

The UCX configuration is stored in the installation folder.

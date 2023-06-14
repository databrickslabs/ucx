# UC Migration Toolkit

This repo contains various functions and utilities for UC Upgrade.


## Latest working version and how-to

Please note that current project statis is 🏗️ **WIP**, but we have a minimal set of already working utilities.
To run the notebooks please use latest LTS Databricks Runtime (non-ML), without Photon, in a single-user cluster mode with UC enabled.

Please note that script is executed only on the driver node, therefore you'll need to use a Single Node Cluster with sufficient amount of cores (e.g. 16 cores).

Recommended VM types are:

- Azure: `Standard_F16`
- AWS: `c4.4xlarge`
- GCP: `c2-standard-16`

**For now please switch to the `v0.0.1` tag in the GitHub to get the latest working version.**


## Local setup and development process

- Install [poetry](https://python-poetry.org/)
- Run `poetry install` in the project directory
- Pin your IDE to use the newly created poetry environment

> Please note that you **don't** need to use `poetry` inside notebooks or in the Databricks workspace.
> It's only introduced to simplify local development.

Before running `git push`, don't forget to link your code with:

```shell
make lint
```


Note: this package uses the Databricks Python SDK for [Authentication](https://github.com/databricks/databricks-sdk-py#authentication) to your Databricks Workspace

### Details of package installation

Since the package itself is managed with `poetry`, to re-use it inside the notebooks we're doing the following:

1. Installing the package dependencies via poetry export
2. Adding the package itself to the notebook via `sys.path`


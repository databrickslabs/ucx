# UC Migration Toolkit

This repo contains various functions and utilities for UC Upgrade.


## Latest working version and how-to

Please note that current project statis is 🏗️ **WIP**, but we have a minimal set of already working utilities.
To run the notebooks please use latest LTS Databricks Runtime (non-ML), without Photon, in a single-user cluster mode.

> If you have Table ACL Clusters or SQL Warehouse where ACL have been defined, you should create a TableACL cluster to run this notebook.

Please note that script is executed **only** on the driver node, therefore you'll need to use a Single Node Cluster with sufficient amount of cores (e.g. 16 cores).

Recommended VM types are:

- Azure: `Standard_F16`
- AWS: `c4.4xlarge`
- GCP: `c2-standard-16`

**For now please switch to the `v0.0.1` tag in the GitHub to get the latest working version.**


## Local setup and development process

- Install [hatch](https://github.com/pypa/hatch):
```shell
pip install hatch
```
- Create environment:
```shell
hatch env create
```
- Install dev dependencies:
```shell
hatch run pip install -e '.[dev]'
```

- Pin your IDE to use the newly created virtual environment. You can get the python path with:

```shell
hatch run python -c "import sys; print(sys.executable)"
```

> Please note that you **don't** need to use `hatch` inside notebooks or in the Databricks workspace.
> It's only introduced to simplify local development.

Before running `git push`, don't forget to link your code with:

```shell
hatch run lint:fmt
```

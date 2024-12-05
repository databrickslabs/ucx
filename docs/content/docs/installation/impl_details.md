---
title: Implementation details
linkTitle: Implementation details
---

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


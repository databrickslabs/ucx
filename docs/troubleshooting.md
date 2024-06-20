# UCX Troubleshooting guide
This guide will help you troubleshoot potential issues running the UCX toolkit.

* [UCX Troubleshooting guide](#ucx-troubleshooting-guide)
  * [Common errors](#common-errors)
  * [Locating error messages](#locating-error-messages)
    * [Databricks workspace](#databricks-workspace)
      * [Databricks jobs](#databricks-jobs)
      * [Databricks notebooks](#databricks-notebooks)
    * [UCX command Line](#ucx-command-line)
    * [UCX Log Files](#ucx-log-files)
      * [Accessing Databricks UCX Logs through the Databricks UI](#accessing-databricks-ucx-logs-through-the-databricks-ui)
      * [Accessing Databricks UCX logs via CLI](#accessing-databricks-ucx-logs-via-cli)
    * [Reading log files](#reading-log-files)
  * [Getting more help](#getting-more-help)
    * [UCX GitHub repository](#ucx-github-repository)
    * [Databricks community](#databricks-community)
    * [Databricks support](#databricks-support)
    * [Databricks partners](#databricks-partners)
  * [Resolving common UCX errors](#resolving-common-ucx-errors)
    * [Cryptic errors on authentication](#cryptic-errors-on-authentication)
    * [Resolving common errors on UCX install](#resolving-common-errors-on-ucx-install)
      * [Error on installing the ucx inventory database](#error-on-installing-the-ucx-inventory-database)
      * [Error installing a wheel file](#error-installing-a-wheel-file)
      * [Error running the assessment job](#error-running-the-assessment-job)
      * [Specific assessment job tasks fail.](#specific-assessment-job-tasks-fail)
    * [Resolving other common errors](#resolving-other-common-errors)

## Common errors
Errors may occur during:
- Installing the `databricks` CLI
- Setting up authentication
- Installing the UCX labs project into a workspace
- Running the UCX Assessment job
- Running other UCX upgrade jobs
- Running upgrade specific task commands via `databricks labs ucx <command>`
- Running and viewing a UCX Dashboard

## Locating error messages
When encountering issues while using UCX with Databricks, the first step is to locate any error messages that may provide clues about the underlying problem. There are a few places to check for error messages:

### Databricks workspace
#### Databricks jobs
If you are running UCX Assessment or other type of UCX job on Databricks, check the job run details page ([docs](https://docs.databricks.com/en/workflows/jobs/monitor-job-runs.html)) of the job for any error messages or status information.

#### Databricks notebooks
If you are using UCX within a Databricks notebook, check the notebook output for any error messages or exceptions.

### UCX command Line
When running UCX commands from the command line, such as databricks labs install ucx, check the terminal output for any error messages or status information. To gather more information on the error, turn on the `--debug` flag, for example:
```sh
databricks labs install ucx --debug
```

### UCX Log Files
UCX generates log files that can provide more detailed information about errors and issues. The location of these log files is discussed in the next section.

#### Accessing Databricks UCX Logs through the Databricks UI
Log in to your Databricks workspace.
Navigate to the "ucx" installation folder (`/Workspace/Applications/ucx`) via the Workspace browser in the Databricks UI.
Locate the logs associated with the UCX-related jobs or notebooks by navigating into the `logs/assessment` folder, then to the latest `run-NNNNNNNNNNNNNN` folder. The logs in the `run-NNNNNNNNNNNNNN` folder are organized by assessment task.
You may find a `README.md` file with backlinks to the Databricks assessment job and job run.

- Download the log files or view them directly in the Databricks UI.

#### Accessing Databricks UCX logs via CLI

Alternatively, you can use the Databricks CLI to access the logs:
```sh
databricks workspace export-dir /Workspace/Applications/ucx/logs logs --profile <WORKSPACEPROFILE>
```
and to verify the download, as an example:
```sh
tree logs
logs
└── assessment
    └── run-288812580605830
        ├── README.md
        ├── assess_azure_service_principals.log
        ├── assess_clusters.log
        ├── assess_global_init_scripts.log
        ├── assess_incompatible_submit_runs.log
        ├── assess_jobs.log
        ├── assess_pipelines.log
        ├── crawl_cluster_policies.log
        ├── crawl_grants.log
        ├── crawl_groups.log
        ├── crawl_mounts.log
        ├── estimate_table_size_for_migration.log
        ├── guess_external_locations.log
        ├── setup_tacl.log
        ├── workspace_listing.log
        ├── workspace_listing.log.2024-03-30_03-19
        ├── workspace_listing.log.2024-03-30_03-29
        ├── workspace_listing.log.2024-03-30_03-39
        └── workspace_listing.log.2024-03-30_03-49
```

### Reading log files
Open the downloaded log files in a text editor or viewer. 

VSCode is an excellent example as it will allow you to search the entire folder for `ERROR`

Scan the logs for any error messages, warnings, or other relevant information that may help you identify the root cause of the issue.
Look for specific error codes, stack traces, or other diagnostic information that can provide clues about the problem.

## Getting more help
If you are unable to resolve the issue using the information in the log files or the troubleshooting steps provided, you can seek additional help from the UCX community or Databricks support:

### UCX GitHub repository
Check the [Databricks UCX GitHub repository](https://github.com/databrickslabs/ucx) for any open issues or discussions related to your problem. You can also create a new issue to seek assistance from the UCX community.

### Databricks community
Engage with the Databricks UCX community via https://github.com/databrickslabs/ucx/issues to ask questions, share your experiences, and seek guidance from other users and experts.

### Databricks support
If you are a Databricks customer, you can reach out to the Databricks account team for assistance with your UCX-related issues. Provide them with the relevant log files and any other diagnostic information to help them investigate the problem.

### Databricks partners
Your account team can direct you to Certified Databricks UC Migration service partners.

By following these steps, you should be able to effectively locate, access, and analyze UCX log files to troubleshoot issues and seek additional help when needed.

## Resolving common UCX errors

### Cryptic errors on authentication
- Ensure that your `DATABRICKS_` environment variables are unset if you are using the `--profile <CONFIGPROFILENAME>` option
- Ensure you do not forget the `--profile <CONFIGPROFILENAME>` to authenticate. If the databricks command cannot authenticate, you may receive a lengthy stack traceback
- Validate your login with `databricks auth env --profile <CONFIGPROFILENAME>` which should print out a json structure having about 8 different keys and values and secrets

If Azure CLI has already been installed and authenticated, but you see the following error when running ucx commands:

`14:50:33 ERROR [d.labs.ucx] In order to obtain AAD token, Please run azure cli to authenticate.`

Resolve this in macOS by running the command with an explicit auth type set: `DATABRICKS_AUTH_TYPE=azure-cli databricks labs ucx ...`. 
To resolve this issue in Windows, proceed with the following steps:

1. Open `%userprofile%` (the path like `C:\Users\<username>`)
2. Open `.databrickscfg`
3. Change the profile definition by setting `auth_type=azure-cli`
4. Save, close the file, re-run `databricks labs ucx ...`

### Resolving common errors on UCX install

#### Error on installing the ucx inventory database
Your platform administrators may have implemented policies in one manner or another to prevent arbitrary database creation. 

-  You may be prohibited from creating a database with a default location to `dbfs:/`.
-  You may be required to create a database on an external Hive Metastore (HMS) and need compute configured to do so.
-  You may need an Instance Profile to create a database in Glue Catalog.
-  You may be prohibited from creating a database in Glue Catalog.
-  You may need a bucket or storage account for the new database.

It would be good to check the log files and in the case of database setup, check the SQL Warehouse [Query History](https://docs.databricks.com/en/sql/user/queries/query-history.html#view-query-history) for queries (e.g. `CREATE DATABASE`, `CREATE TABLE`, `CREATE VIEW`) with errors.

In these cases where it's not immediately possible as a workspace admin to create a Hive Metastore (HMS) database, ask your DBA, data admins or cloud admins to create a database. UCX will create close to twenty tables to store assessment information into the database.

#### Error installing a wheel file
Access to DBFS may be restricted and loading wheel files into DBFS may be restricted. For now, check the GitHub issues for potential resolution. More information will be added shortly.

#### Error running the assessment job
Often the compute type available is restricted, or it needs a special `spark` configuration or Instance Profile.
- You can either create cluster policies, and then re-install; see the installation guide for more information.
- Or, as needed, create two interactive clusters (Legacy Unassigned cluster and Legacy Table ACL cluster) and manually reconfigure the assessment job.

#### Specific assessment job tasks fail.
See the gathering log information sections elsewhere in this document.

### Resolving other common errors
-  If you have an external Hive Metastore (HMS) such as Glue Catalog or a MySQL, Postgres or SQL server database, please consult the [External Hive Metastore Integration guide](external_hms_glue.md)
-  If you are running table upgrade commands and workflows. Please consult the [Table Upgrade guide](table_upgrade.md)
-  If you are trying to understand the Assessment report, please consult the [Assessment documentation](assessment.md)

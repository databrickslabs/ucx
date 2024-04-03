# UCX Troubleshooting Guide
This guide will help you trouble shoot potential issues running the UCX toolkit.

## Common Errors
Errors may occur during:
- Installing the `databricks` CLI
- Setting up authentication
- Installing the UCX labs project into a workspace
- Running the UCX Assessment job
- Running other UCX upgrade jobs
- Running upgrade specific task commands via `databricks labs ucx <command>`
- Running and viewing a UCX Dashboard

## Locating Error Messages
When encountering issues while using UCX with Databricks, the first step is to locate any error messages that may provide clues about the underlying problem. There are a few places to check for error messages:

### Databricks Workspace
#### Databricks Jobs: 
If you are running UCX Assessment or other type of UCX job on Databricks, check the job run details page ([docs](https://docs.databricks.com/en/workflows/jobs/monitor-job-runs.html)) of the job for any error messages or status information.

#### Databricks Notebooks: 
If you are using UCX within a Databricks notebook, check the notebook output for any error messages or exceptions.

### UCX Command Line
UCX CLI Commands: When running UCX commands from the command line, such as databricks labs install ucx, check the terminal output for any error messages or status information.

### UCX Logs Files:
UCX generates log files can provide more detailed information about errors and issues. The location of these log files is discussed in the next section.

#### Accessing Databricks UCX Logs through the Databricks UI
Log in to your Databricks workspace.
Navigate to the "ucx" installation folder (`/Workspace/Applications/ucx`) via the Workspace browser in the Databricks UI.
Locate the logs associated with the UCX-related jobs or notebooks by navigating into the `logs/assessment` folder, then to the latest `run-NNNNNNNNNNNNNN` folder. The logs in the `run-NNNNNNNNNNNNNN` folder are organized by assessment task.
You may find a `README.md` file with back links to the Databricks assessment job and job run.

- Download the log files or view them directly in the Databricks UI.

#### Accessing Databricks UCX Logs via CLI

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

### Reading Log Files
Open the downloaded log files in a text editor or viewer. 

VSCode is an excellent example as it will allow you to search the entire folder for `ERROR`

Scan the logs for any error messages, warnings, or other relevant information that may help you identify the root cause of the issue.
Look for specific error codes, stack traces, or other diagnostic information that can provide clues about the problem.

## Getting More Help
If you are unable to resolve the issue using the information in the log files or the troubleshooting steps provided, you can seek additional help from the UCX community or Databricks support:

### UCX GitHub Repository: 
Check the [Databricks UCX GitHub repository](https://github.com/databrickslabs/ucx) for any open issues or discussions related to your problem. You can also create a new issue to seek assistance from the UCX community.

### Databricks Community: 
Engage with the Databricks UCX community via https://github.com/databrickslabs/ucx/issues to ask questions, share your experiences, and seek guidance from other users and experts.

### Databricks Support
If you are a Databricks customer, you can reach out to Databricks account team for for assistance with your UCX-related issues. Provide them with the relevant log files and any other diagnostic information to help them investigate the problem.

### Databricks Partners
Your account team can direct you to Certified Databricks UC Migration service partners.

By following these steps, you should be able to effectively locate, access, and analyze UCX log files to troubleshoot issues and seek additional help when needed.

## Fixing common errors
### Cryptic errors on authentication
- Ensure that your `DATABRICKS_` environment variables are unset if you are using the `--profile <CONFIGPROFILENAME>` option
- Ensure you did not forget the `--profile <CONFIGPROFILENAME>` to authenticate. If the databricks command cannot authenticate, you may receive a lengthy stack traceback
- Validate your login with `databricks auth env --profile <CONFIGPROFILENAME>`

### Resolving common errors on UCX install

#### Error on installing the ucx inventory database
Your platform adminsitrators may have implmented policies in one manner or another to prevent arbitrary database creation. 
- It may be prohibitive to create a database that stores data on DBFS.
- You may be required to create a database on an external Hive Meta Store
- You may need an IAM role to create a database in Glue Catalog.
- You may be prohibited from creating a database in Glue Catalog.
- You may need a bucket or storage account for the new database.
In these cases where it's not immediately available as a workspace admin to create a HMS database, ask your DBA or data admins to create a database. UCX will create close to twenty tables to capture assessment information into it.

#### Error Installing a Wheel file.
Access to DBFS may be restricted and loading wheel files into DBFS may be restricted. For now, check the github issues for potential resolution.

#### Error running the assessment job
Often the compute type available is restricted, or it needs a special configuration.
You can either create a cluster policies and re-install, see the installation guide.
Or, as needed or create two interactive clusters (Legacy Unassigned cluster and Legacy Table ACL clustger) and manually re-configure the assessment job.

#### Specific assessment job tasks fail.
See the gathering log information sections elsewhere in this document.
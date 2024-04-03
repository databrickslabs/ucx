# UCX Troubleshooting Guide
This guide will help you trouble shoot potential issues running the UCX toolkit.

## Common Errors
Errors often occur during:
- Installing the `databricks` CLI
- Setting up authentication
- Installing the UCX labs project into a workspace
- Running the Assessment job
- Running other jobs
- Running upgrade specific task commands via `databricks labs ucx <command>`
- Running and viewing a UCX Dashboard

## Locating Error Messages
When encountering issues while using UCX with Databricks, the first step is to locate any error messages that may provide clues about the underlying problem. There are a few places to check for error messages:

### Databricks Workspace
Databricks Jobs: If you are running UCX Assessment or other type of UCX job on Databricks, check the job run details page of the job for any error messages or status information.
Databricks Notebooks: If you are using UCX within a Databricks notebook, check the notebook output for any error messages or exceptions.

### UCX Command Line
UCX CLI Commands: When running UCX commands from the command line, such as databricks labs install ucx, check the terminal output for any error messages or status information.

### UCX Logs Files:
UCX generates log files that can provide more detailed information about errors and issues. The location of these log files is discussed in the next section.

#### Accessing Databricks UCX Logs through the Databricks UI
Log in to your Databricks workspace.
Navigate to the "ucx" installation folder (`/Workspace/Applications/ucx`) via the Workspace browser in the Databricks UI.
Locate the logs associated with the UCX-related jobs or notebooks by navigating into the `logs/assessment` folder, then to the latest `run-NNNNNNNNNNNNNN` folder. The logs in the `run-NNNNNNNNNNNNNN` folder are organized by assessment task.
You may find a `README.md` file with back links to the Databricks assessment job and job run.

- Download the log files or view them directly in the Databricks UI.

#### Accessing Databricks UCX Logs via CLI

Alternatively, you can use the Databricks CLI to access the logs:
Install and configure the Databricks CLI on your local machine.
Run the databricks logs get command to download the logs for a specific job or notebook.
Downloading and Reading Log Files
Once you have identified the location of the UCX log files, you can download and read them to gather more information about the issues you are experiencing.

### Downloading Log Files
If the logs are stored locally, simply copy or download the log files to your local machine.
If the logs are stored in the Databricks environment, use the Databricks CLI or the Databricks UI to download the log files.

### Reading Log Files
Open the downloaded log files in a text editor or viewer.
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
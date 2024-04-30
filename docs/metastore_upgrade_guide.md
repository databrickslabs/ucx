## Table Upgrade Guide
This document provides a guide for upgrading the Hive metastore objects to UC using UCX.

## Prerequisites
For UCX to be able to upgrade the metastore. The following prerequisites must be met:
1. UCX must be installed and configured on the workspace. For more information on how to install UCX, refer to the [UCX Readme](../README.md).
2. In case of an external metastore (such as GLUE), UCX has to be configured to attach to the metastore. For more information on how to configure UCX to attach to an external metastore, refer to the [UCX Readme](../README.md).
3. The assessment workflow must be run.
4. It is recommended that the group migration process will be completed before upgrading the metastore.

## Upgrade Process
The upgrade process is done in multiple steps. For each step we will discuss the manual process and how to perform it using UCX.

### Step 1: Mapping Metastore Tables (UCX Only)
In this step we will map the metastore tables to UC tables.
This step can be performed using the `create-table-mapping` command documented in the [UCX Readme](../README.md#create-table-mapping-command).

Update the mapping file with the required mappings. That can be performed by editing the file that was created in the previous step.

### Step 2: Create the necessary cloud principals for the upgrade
This step has multiple sub steps and should be performed by a cloud account administrator.
We rely on CLI access (AWS) or API Access (Azure) to create the necessary cloud principals.
In the future we will add support for creating the necessary cloud principals using Terraform.

To understand this step it is important to understand how Databricks accesses cloud locations.
You can read about it here:
[AWS - Create External Locations](https://docs.databricks.com/en/connect/unity-catalog/external-locations.html)
[Azure - Create External Locations](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/external-locations)

#### Step 2.1: Map the cloud principals to the cloud "prefixes"
In this step we are going to map all the cloud principals  

#### Step 2.2: Create/Modify Cloud Principals

#### Step 2.3: Create Credentials

#### Step 2.4: Create External Locations


### Step 3: Upgrade the Metastore
Upgrading the metastore is done in steps.
Each step can be executed separately as a standalone command.
Each step represents a different type of metastore object.
We identified the following object types.
* **EXTERNAL_SYNC**<br>
  Tables not saved to the DBFS file system that are supported by the sync command.<br>
  These tables are in one of the following formats: DELTA, PARQUET, CSV, JSON, ORC, TEXT, AVRO<br>
  More information about the sync command can be found [here](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-sync.html)<br>
  During the upgrade process, these table contents will remain intact and the metadata will be recreated in UC.
* EXTERNAL_HIVESERDE = auto()
* **EXTERNAL_NO_SYNC**<br>
    Tables not saved to the DBFS file system that are not supported by the sync command.<br>
    The current upgrade process will migrate these tables to UC by creating a new managed table in UC and copying the data from the old table to the new table.
    The new table's format will be Delta.
  * **DBFS_ROOT_DELTA**<br>
      Tables saved to the DBFS file system that are in Delta format.<br>
      The current upgrade process will create a copy of these tables in UC using the "deep clone" command.
      More information about the deep clone command can be found [here](https://docs.databricks.com/en/sql/language-manual/delta-clone.html)<br>
* DBFS_ROOT_NON_DELTA = auto()
* VIEW = auto()



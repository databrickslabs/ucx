## Table Upgrade Guide
This document provides a guide for upgrading the Hive metastore objects to UC using UCX.
The metastore upgrade process is composed of multiple steps.
To effectively upgrade the metastores four principal operations are required:
1. Assess - In this step, you will evaluate the existing HMS tables identified for upgrade so that we can determine the right approach for upgrade. This step is a prerequisite and is performed by the assessment workflow.
2. Create - In this step, you create the required UC assets such as, Metastore, Catalog, Schema, Storage Credentials, External Locations. This step is part of the upgrade process.
3. Upgrade/Grant these are two steps that UCX combine.
   4. Upgrade - The metastores objects (tables/views) will be converted to a format supported by UC 
   4. Grant - You will need to provide grants on the newly upgraded UC tables to principals, so that they can access the UC tables.

## Prerequisites
For UCX to be able to upgrade the metastore. The following prerequisites must be met:
1. UCX must be installed and configured on the workspace. For more information on how to install UCX, refer to the [UCX Readme](../README.md).
2. In case of an external metastore (such as GLUE), UCX has to be configured to attach to the metastore. For more information on how to configure UCX to attach to an external metastore, refer to the [UCX Readme](../README.md).
3. The assessment workflow must be run.
4. It is recommended that the group migration process will be completed before upgrading the metastore. For more information on how to migrate groups, refer to the [UCX Readme](../README.md).
5. The workspace should be configured with a Metastore follow the instruction here [Create UC Metastore](https://docs.databricks.com/en/data-governance/unity-catalog/create-metastore.html)<br>
   Metastore can be attached to the workspace using the following UCX command:<br>

## Upgrade Process
The upgrade process is done in multiple steps. For each step we will discuss the manual process and how to perform it using UCX.

### Step 1: Mapping Metastore Tables (UCX Only)
In this step we will map the metastore tables to UC tables.
#### Step 1.1: Create the mapping file
This step can be performed using the `create-table-mapping` command documented in the [UCX Readme](../README.md#create-table-mapping-command).


#### Step 1.2: Update the mapping file
Update the mapping file with the required mappings. That can be performed by editing the file that was created in the previous step.




### Step 2: Create the necessary cloud principals for the upgrade
This step has multiple sub steps and should be performed by a cloud account administrator.
We rely on CLI access (AWS) or API Access (Azure) to create the necessary cloud principals.
In the future we will add support for creating the necessary cloud principals using Terraform.

We refer to principals that has to be created/modified during the process.
Principal are:
Roles/Instance Profiles in AWS
Service Principals/Managed Identities in Azure

To understand this step it is important to understand how Databricks accesses cloud locations.
You can read about it here:
[AWS - Create External Locations](https://docs.databricks.com/en/connect/unity-catalog/external-locations.html)
[Azure - Create External Locations](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/external-locations)

#### Step 2.1: Map the cloud principals to the cloud "prefixes"
In this step we are going to map all the cloud principals to the paths they have access to.


#### Step 2.2: Create/Modify Cloud Principals


#### Step 2.3: Create Credentials

#### Step 2.4: Create External Locations

#### Step 2.5: Create "Uber Principal"
Uber Principals are principals that have access to all the external tables' location. 
They are "Legacy" principals and not required to support UC. The purpose of these roles is for the cluster that performs the upgrade to have access to all the tables in HMS.
Once the upgrade is completed, these principals can (and should) be deleted.

#### Step 2.6: Create the 


### Step 3: Upgrade the Metastore
Upgrading the metastore is done in steps.
Each step can be executed separately as a standalone command.
Each step represents a different type of metastore object.
We identified the following object types.
Each of the object that is being upgraded will be marked with an `upgraded_to` property.
This property will be used to skip the object in the next upgrade runs.
It also points to the location of the upgraded object in UC.
Each of the upgraded objects will be marked with an `upgraded_from` property.
This property will be used to identify the original location of the object in the metastore.
We also add a `upgraded_from_workspace_id` property to the upgraded object, to identify the source workspace.

* **EXTERNAL_SYNC**<br>
  Tables not saved to the DBFS file system that are supported by the sync command.<br>
  These tables are in one of the following formats: DELTA, PARQUET, CSV, JSON, ORC, TEXT, AVRO<br>
  More information about the sync command can be found [here](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-sync.html)<br>
  During the upgrade process, these table contents will remain intact and the metadata will be recreated in UC.
* **EXTERNAL_HIVESERDE**<br>
  For table with table type "HIVE" upgrade can be in-place migration or CTAS.
  We provide two workflows for hiveserde table migration:<br>
  1. One will migrate all hiveserde tables using CTAS which we officially support.
  2. The other one will migrate certain types of hiveserde in place, which is technically working, but the user
  need to accept the risk that the old files created by hiveserde may not be processed correctly by Spark
  datasource in corner cases.
  User will need to decide which workflow to runs first which will migrate the hiveserde tables and mark the
  `upgraded_to` property and hence those tables will be skipped in the migration workflow runs later.
* **EXTERNAL_NO_SYNC**<br>
    Tables not saved to the DBFS file system that are not supported by the sync command.<br>
    The current upgrade process will migrate these tables to UC by creating a new managed table in UC and copying the data from the old table to the new table.
    The new table's format will be Delta.
* **DBFS_ROOT_DELTA**<br>
    Tables saved to the DBFS file system that are in Delta format.<br>
    The current upgrade process will create a copy of these tables in UC using the "deep clone" command.
    More information about the deep clone command can be found [here](https://docs.databricks.com/en/sql/language-manual/delta-clone.html)<br>
* **DBFS_ROOT_NON_DELTA**<br>
* **VIEW**
      Views are recreated during the upgrade process. The view's definition will be modified to repoint to the new UC tables.
      Views should be migrated only after all the dependent tables have been migrated.
      The upgrade process account for View to View dependencies.

#### Step 4: Odds and Ends
The following steps can be used to repair/amend the metastore after the upgrade process.

#### Step 4.1: Moving objects


#### Step 4.2: Reverting objects





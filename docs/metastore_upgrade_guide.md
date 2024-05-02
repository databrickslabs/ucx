## Table Upgrade Guide
This document provides a guide for upgrading the Hive metastore objects to UC using UCX.
The metastore upgrade process is composed of multiple steps.
To effectively upgrade the metastores four principal operations are required:
1. Assess - In this step, you will evaluate the existing HMS tables identified for upgrade so that we can determine the right approach for upgrade. This step is a prerequisite and is performed by the assessment workflow.
2. Create - In this step, you create the required UC assets such as, Metastore, Catalog, Schema, Storage Credentials, External Locations. This step is part of the upgrade process.
3. Upgrade/Grant these are two steps that UCX combine.
   4. Upgrade - The metastores objects (tables/views) will be converted to a format supported by UC 
   4. Grant - The table upgrade the newly created object the same permission as the original object.

## Prerequisites
For UCX to be able to upgrade the metastore. The following prerequisites must be met:
1. UCX must be installed and configured on the workspace. For more information on how to install UCX, refer to the [external_hms_glue.md](external_hms_glue.md).
2. In case of an external metastore (such as GLUE), UCX has to be configured to attach to the metastore. For more information on how to configure UCX to attach to an external metastore, refer to the [External Metastore Guide]().
3. The assessment workflow must be run.
4. It is recommended that the group migration process will be completed before upgrading the metastore. For more information on how to migrate groups, refer to the [UCX Readme](../README.md).
5. The workspace should be configured with a Metastore follow the instruction here [Create UC Metastore](https://docs.databricks.com/en/data-governance/unity-catalog/create-metastore.html)<br>
   Metastore can be attached to the workspace using the following UCX command:<br>
   ```text
    databricks labs ucx assign-metastore --workspace-id <workspace-id> [--metastore-id <metastore-id>]
    ```

## Upgrade Process
The upgrade process is done in multiple steps. For each step we will discuss the manual process and how to perform it using UCX.

### Step 1: Mapping Metastore Tables (UCX Only)
In this step we will map the metastore tables to UC tables.
#### Step 1.1: Create the mapping file
This step can be performed using the `create-table-mapping` command documented in the [UCX Readme](../README.md#create-table-mapping-command).
CLI Command
```text
databricks labs ucx create-table-mapping 
```

#### Step 1.2: Update the mapping file
Update the mapping file with the required mappings. That can be performed by editing the file that was created in the previous step.
By default all the tables/views will be mapped to UC tables.
All the tables will be mapped to a single catalog, maintaining the schema/name of the original table.
You can exclude tables from the mapping by removing the line from the mapping file.
You can also change the catalog/schema name of the UC table by changing the line in the mapping file.

The CLI command will create a mapping file in the workspace UCX folder.
The mapping file is in CSV format and can be edited using any text editor or Excel.

The format of the mapping file is as follows:

| **columns:** | **workspace_name**      | **catalog_name** | **src_schema** | **dst_schema** | **src_table** | **dst_table** |
|--------------|---------------------|--------------|----------------|----------------|---------------|---------------|
| values:      | data_engineering_ws | de_catalog   | database1      | database1      | table1        | table1        |

The

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


#### Step 2.2: Create/Modify Cloud Principals and Credentials
In this step we will create the necessary cloud principals for the UC credentials.
The manual process is documented in the following links:
[AWS-Storage Credentials](https://docs.databricks.com/en/connect/unity-catalog/storage-credentials.html)
[Azure-Storage Credentials](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-storage-credentials)

For both AWS and Azure we can use the following CLI command to upgrade the necessary cloud principals:
```text
databricks labs ucx migrate-credentials
```

Azure: this command migrates Azure Service Principals, which have Storage Blob Data Contributor,
Storage Blob Data Reader, Storage Blob Data Owner roles on ADLS Gen2 locations that are being used in
Databricks, to UC storage credentials.
The Azure Service Principals to location mapping are listed in
{workspace ucx folder}/azure_storage_account_info.csv which is generated by principal_prefix_access command.
Please review the file and delete the Service Principals you do not want to be migrated.
The command will only migrate the Service Principals that have client secret stored in Databricks Secret.

AWS: this command migrates AWS Instance Profiles that are being used in Databricks, to UC storage credentials.
The AWS Instance Profiles to location mapping are listed in
{workspace ucx folder}/aws_instance_profile_info.csv which is generated by principal_prefix_access command.
Please review the file and delete the Instance Profiles you do not want to be migrated.
Pass aws_profile for aws.

For AWS we have the option to create fresh new AWS roles and set them up for UC access, using the following command:
```text
databricks labs ucx create-missing-principles --aws-profile <aws_profile> --single-role <single_role>
```
This command identifies all the S3 locations that are missing a UC compatible role and creates them. 
It takes single-role optional parameter. 
If set to True, it will create a single role for all the S3 locations.
Otherwise, it will create a role for each S3 location.


#### Step 2.3: Create Credentials
Once the cloud principals are created, we can create the UC credentials.
The manual process is documented in the following links:
[AWS-Storage Credentials](https://docs.databricks.com/en/connect/unity-catalog/storage-credentials.html)
[Azure-Storage Credentials](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-storage-credentials)

AWS and Azure:
The following CLI command can be used to create the UC credentials:
```text
databricks labs ucx create-credentials
```


#### Step 2.4: Create External Locations
Once the UC credentials are created, we can create the UC external locations.
An external location will be created for each of the locations identified in the assessment.
The Assessment dashboard displayed all the locations that need to be created.
The Manual process is documented in the following links:
[AWS - Create External Locations](https://docs.databricks.com/en/connect/unity-catalog/external-locations.html)
[Azure - Create External Locations](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/external-locations)

#### Step 2.5: Create "Uber Principal"
Uber Principals are principals that have access to all the external tables' location. 
They are "Legacy" principals and not required to support UC. The purpose of these roles is for the cluster that performs the upgrade to have access to all the tables in HMS.
Once the upgrade is completed, these principals can (and should) be deleted.

#### Step 2.6: Create Catalogs and Schemas 
In this step we will create the UC catalogs and schemas required for the target tables.
The following CLI command can be used to create the UC catalogs and schemas:
```text
databricks labs ucx create-catalogs-schemas
```
The command will create the UC catalogs and schemas based on the mapping file created in the previous step.


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

The upgrade process can be triggered using the following command:
```text
databricks labs ucx migrate-tables
```

Or by running the "Migrate Tables" workflow deployed to the workspace.

### Step 4: Odds and Ends
The following steps can be used to repair/amend the metastore after the upgrade process.

#### Step 4.1: Skipping Table/Schema
```text
databricks labs ucx skip --schema X [--table Y]  
```
This command will mark the table or schema as skipped. The table will not be upgraded in the next run of the upgrade process.


#### Step 4.2: Moving objects
```text
databricks labs ucx move --from-catalog A --from-schema B --from-table C --to-catalog D --to-schema E  
```
This command will move the object from the source location to the target location. 
The `upgraded_from` property will be updated to reflect the new location on the source object.
This command should be used in case the object was created in the wrong location.
#### Step 4.2: Aliasing objects
```text
databricks labs ucx alias --from-catalog A --from-schema B --from-table C --to-catalog D --to-schema E  
```
This command will create an alias for the object in the target location. It will create a view for tables that need aliasing.
It will create a mirror view to view that is marked as alias.
The use of this command is in case we need multiple identical tables or views in multiple locations.
HMS allows creating multiple tables pointing to the same location. 
UC does not support creating multiple tables pointing to the same location, thus we need to create an alias for the table.
#### Step 4.3: Reverting objects
```text
databricks labs ucx revert-migrated-tables --schema X --table Y [--delete-managed]  
```
This command will remove the upgraded table and reset the `upgraded_from` property. It will allow for upgrading the table again.





# Assessment Report Summary
The Assessment Report (Main) is the output of the Databricks Labs UCX assessment workflow. This report queries the $inventory database (e.g. `ucx`) and summarizes the findings of the assessment. The link to the Assessment Report (Main) can be found in your home folder, under `.ucx` in the README.py file. The user may also directly navigate to the Assessment report by clicking on `Dashboards` icon on the left to find the Dashboard.

# Metrics boxes
## Readiness
This is an overall summary of rediness detailed in the Readiness dashlet. This value is based on the ratio of findings divided by the total number of assets scanned.

## Total Databases
The total number of `hive_metastore` databases found during the assessment.

## Metastore Crawl Failures
Total number of failures encountered by the crawler while extracting metadata from the Hive Metastore and REST APIs.

## Total Tables
Total number of hive metastore tables discovered

## Storage Locations
Total number of identified storage locations based on scanning Hive Metastore tables and schemas


# Assessment Dashlets
## Readiness
This is a rough summary of the workspace readiness to run Unity Catalog governed workloads. Each line item is the percent of compatible items divided by the total items in the class.

## Assessment Summary
This is a summary count, per finding type of all of the findings identified during the assessment workflow. The assessment summary will help identify areas that need focus (e.g. Tables on DBFS or Clusters that need DBR upgrades)

## Database Summary
This is a Hive Metastore based Database by Database assessment summary along with an upgrade strategy.
`In Place Sync` indicates that the `SYNC` command can be used to copy the metadata into a Unity Catalog Catalog.

## External Locations
Tables were scanned for `LOCATION` attributes and that list was distilled down to External Locations. In Unity Catalog, create a STORAGE CREDENTIAL that can access the External Locations, then define UC `EXTERNAL LOCATION`s for these items.

## Mount Points
Mount points are popular means to provide access to external buckets / storage accounts. A more secure form in Unity Catalog are EXTERNAL LOCATIONs and VOLUMES. EXTERNAL LOCATIONs are the basis for EXTERNAL Tables, Schemas, Catalogs and VOLUMES. VOLUMES are the basis for managing files. The recommendation is to migrate Mountpoints to Either EXTERNAL LOCATIONS or VOLUMEs.

## Incompatible Clusters
This is a list of findings (reasons) and clusters that may need upgrading. See Assessment Finding Index (below) for specific recommendations.

## Incompatible Jobs
This is a list of findings (reasons) and jobs that may need upgrading. The reasons are often due to incompatible (non Unity Catalog) compute types. See Assessment Findings Index for more information.

## Incompatible Delta Live Tables
These are Delta Live Table jobs that may be incompatible with Unity Catalog

## Incompatible Global Init Scripts
These are Global Init Scripts that are incompatible with Unity Catalog compute. As a reminder, global init scripts need to be on secure storage (Volumes or a Cloud Storage account and not DBFS)


# Assessment Finding Index

This section will help explain UCX Assessment findings and provide a recommended action

### AF000 - not supported DBR: ##.#.x-scala2.12
Short description: The compute runtime does not meet the requirements to use Unity Catalog.
Explanation: Unity Catalog capabilities are fully enabled on Databricks Runtime 13.3 LTS. This is the current recommended runtime for production interactive clusters and jobs. This finding is noting the cluster or job compute configuration does not meet this threshold. 
recommendation: Upgrade the DBR version to 13.3 LTS or later.

### AF000 - not supported DBR: ##.#.x-cpu-ml-scala2.12
Currently, MLR (Machine Learning Runtime) and GPU clusters are not supported with Unity Catalog.

### AF000 - not supported DBR: ##.#.x-gpu-ml-scala2.12
Currently, MLR (Machine Learning Runtime) and GPU clusters are not supported with Unity Catalog.

### AF000 - Inplace Sync
Short description: We found that the table or database can be SYNC'd without moving data because the data is stored directly on cloud storage specified via a mount or a cloud storage URL (not DBFS).
How: Run the SYNC command on the table or schema.  If the tables (or source database) is 'managed' first set this spark setting in your session or in the interactive cluster configuration: `spark.databricks.sync.command.enableManagedTable=true`

### AF000 - Asset Replication Required
We found that the table or database needs to have the data copied into a Unity Catalog managed location or table.
Recommendation: Perform a 'deep clone' operation on the table to copy the files 
```sql
CREATE TABLE [IF NOT EXISTS] table_name
   [SHALLOW | DEEP] CLONE source_table_name [TBLPROPERTIES clause] [LOCATION path]   
```

### AF000 - Data in DBFS Root
A table or schema refers to a location in DBFS and not a cloud storage location.
The data must be moved from DBFS to a cloud storage location or to a Unity Catalog managed storage.

### AF000 - Data is in DBFS Mount
A table or schema refers to a location in DBFS mount and not a direct cloud storage location.
Mounts are not suppored in Unity Catalog so the mount source location must be de-referenced and the table/schema objects mapped to a UC external location.

### AF000 - Non-DELTA format: CSV
Unity Catalog does not support managed CSV tables. Recommend converting the table to DELTA format or migrating the table to an External table.

### AF000 - Non-DELTA format: DELTA
This is a known issue of the UCX assessment job

### AF000 - Non-DELTA format: [PARQUET|JDBC|ORC|XML|JSON|HIVE|deltaSharing|com.databricks.spark.csv|...]
Unity Catalog managed tables only support DELTA format.
Recommend converting the table to DELTA lake format, converting the table to an External table.
For `deltaSharing` use Databricks to Databricks Delta Sharing if the provider is also on Databricks.
HIVE type tables are not supported.

For JDBC data sources:

Problem (on shared clusters):
Accessing third-party databases—other than MySQL, PostgreSQL, Amazon Redshift, Snowflake, Microsoft SQL Server, Azure Synapse (SQL Data Warehouse) and Google BigQuery—will require additional permissions on a shared cluster if the user is not a workspace admin. This is due to the drivers not guaranteeing user isolation, e.g., as the driver writes data from multiple users to a widely accessible temp directory.

Workaround:
Granting ANY FILE permissions will allow users to access untrusted databases. Note that ANY FILE will still enforce ACLs on any tables or external (storage) locations governed by Unity Catalog.
Upgrade the DBR runtime to 13.3 LTS or higher to avoid cluster level firewall restrictions.


### AF000 - Unsupported Storage Type: [adl:// | wasb:// | wasbs://]
ADLS Gen 2 (`abfss://`) is the only Azure native storage type supported. Use a Deep Clone process to copy the table data.
```sql
CREATE TABLE [IF NOT EXISTS] table_name
   [SHALLOW | DEEP] CLONE source_table_name [TBLPROPERTIES clause] [LOCATION path]   
```

### AF000 - Uses azure service principal credentials config in cluster.
Azure service principles are replaced by Storage Credentials to access cloud storage accounts.
Create a storage CREDENTIAL, then an EXTERNAL LOCATION and possibly external tables to provide data access.
If the service principal is used to access additional azure cloud services, convert the cluster to a `Assigned` cluster type which *may* work.

### AF000 - Uses azure service principal credentials config in Job cluster.
Azure service principles are replaced by Storage Credentials to access cloud storage accounts.
Create a storage CREDENTIAL, then an EXTERNAL LOCATION and possibly external tables to provide data access.
If the service principal is used to access additional azure cloud services, convert the job cluster to a `Assigned` cluster type which *may* work.

### AF000 - Uses azure service principal credentials config in pipeline.
Azure service principles are replaced by Storage Credentials to access cloud storage accounts.
Create a storage CREDENTIAL, then an EXTERNAL LOCATION and possibly external tables to provide data access.

### AF000 - unsupported config
A spark config option was found in a cluster compute definition that is incompatible with Unity Catalog based compute. The recommendation is to remove or alter the config. Additionally, Unity Catalog enabled clusters may require a different approach to the same capability. As a transition strategy, "Unassigned" clusters or "Assigned" (including job clusters but not shared clusters) may work.
- `spark.hadoop.javax.jdo.option.ConnectionURL` an external Hive Metastore is in use. Ideally migrate those tables and schemas to Unity Catalog where they can be shared across workspaces

### AF000 - unsupported config: spark.databricks.passthrough.enabled
Passthrough security model is not supported by Unity Catalog. Passthrough mode relied upon file based authorization which is incompatible with Fine Grained Access Controls supported by Unity Catalog.
Recommend mapping your Passthrough security model to a External Location/Volume/Table/View based security model compatible with Unity Catalog.

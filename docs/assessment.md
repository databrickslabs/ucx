Migration Assessment Report
===

<!-- TOC -->
* [Migration Assessment Report](#migration-assessment-report)
* [Assessment Report Summary](#assessment-report-summary)
* [Assessment Widgets](#assessment-widgets)
  * [Readiness](#readiness)
  * [Total Databases](#total-databases)
  * [Metastore Crawl Failures](#metastore-crawl-failures)
  * [Total Tables](#total-tables)
  * [Storage Locations](#storage-locations)
* [Assessment Widgets](#assessment-widgets-1)
  * [Readiness](#readiness-1)
  * [Assessment Summary](#assessment-summary)
  * [Table counts by storage](#table-counts-by-storage)
  * [Table counts by schema and format](#table-counts-by-schema-and-format)
  * [Database Summary](#database-summary)
  * [External Locations](#external-locations)
  * [Mount Points](#mount-points)
  * [Table Types](#table-types)
  * [Incompatible Clusters](#incompatible-clusters)
  * [Incompatible Jobs](#incompatible-jobs)
  * [Incompatible Delta Live Tables](#incompatible-delta-live-tables)
  * [Incompatible Global Init Scripts](#incompatible-global-init-scripts)
* [Assessment Finding Index](#assessment-finding-index)
    * [AF101 - not supported DBR: ##.#.x-scala2.12](#af101---not-supported-dbr-x-scala212)
    * [AF102 - not supported DBR: ##.#.x-cpu-ml-scala2.12](#af102---not-supported-dbr-x-cpu-ml-scala212)
    * [AF103 - not supported DBR: ##.#.x-gpu-ml-scala2.12](#af103---not-supported-dbr-x-gpu-ml-scala212)
    * [AF111 - Uses azure service principal credentials config in cluster.](#af111---uses-azure-service-principal-credentials-config-in-cluster)
    * [AF112 - Uses azure service principal credentials config in Job cluster.](#af112---uses-azure-service-principal-credentials-config-in-job-cluster)
    * [AF113 - Uses azure service principal credentials config in pipeline.](#af113---uses-azure-service-principal-credentials-config-in-pipeline)
    * [AF114 - unsupported config](#af114---unsupported-config)
    * [AF115 - unsupported config: spark.databricks.passthrough.enabled](#af115---unsupported-config-sparkdatabrickspassthroughenabled)
    * [AF116 - No isolation shared clusters not supported in UC](#af116---no-isolation-shared-clusters-not-supported-in-uc)
    * [AF117 - cluster type not supported](#af117---cluster-type-not-supported)
    * [AF201 - Inplace Sync](#af201---inplace-sync)
    * [AF202 - Asset Replication Required](#af202---asset-replication-required)
    * [AF203 - Data in DBFS Root](#af203---data-in-dbfs-root)
    * [AF204 - Data is in DBFS Mount](#af204---data-is-in-dbfs-mount)
    * [AF210 - Non-DELTA format: CSV](#af210---non-delta-format-csv)
    * [AF211 - Non-DELTA format: DELTA](#af211---non-delta-format-delta)
    * [AF212 - Non-DELTA format](#af212---non-delta-format)
    * [AF221 - Unsupported Storage Type](#af221---unsupported-storage-type)
* [Common Terms](#common-terms)
  * [UC](#uc)
  * [DELTA](#delta)
  * [CTAS](#ctas)
  * [DEEP CLONE](#deep-clone)
  * [EXTERNAL LOCATION](#external-location)
  * [STORAGE CREDENTIAL](#storage-credential)
<!-- TOC -->

This document describes the Assessment Report generated from the UCX tools. The main assessment report includes dashlets, widgets and details of the assessment findings and common recommendations made based on the Assessment Finding (AF) Index entry.

![report](assessment-report.png)

# Assessment Report Summary
The Assessment Report (Main) is the output of the Databricks Labs UCX assessment workflow. This report queries the $inventory database (e.g. `ucx`) and summarizes the findings of the assessment. The link to the Assessment Report (Main) can be found in your home folder, under `.ucx` in the README.py file. The user may also directly navigate to the Assessment report by clicking on `Dashboards` icon on the left to find the Dashboard.

[[back to top](#migration-assessment-report)]

# Assessment Widgets
<img width="1655" alt="image" src="https://github.com/databrickslabs/ucx/assets/1122251/808f7c68-fcc7-4caa-bab2-03f49a382256">

[[back to top](#migration-assessment-report)]

## Readiness
This is an overall summary of rediness detailed in the Readiness dashlet. This value is based on the ratio of findings divided by the total number of assets scanned.

[[back to top](#migration-assessment-report)]

## Total Databases
The total number of `hive_metastore` databases found during the assessment.

[[back to top](#migration-assessment-report)]

## Metastore Crawl Failures
Total number of failures encountered by the crawler while extracting metadata from the Hive Metastore and REST APIs.

[[back to top](#migration-assessment-report)]

## Total Tables
Total number of hive metastore tables discovered

[[back to top](#migration-assessment-report)]

## Storage Locations
Total number of identified storage locations based on scanning Hive Metastore tables and schemas

[[back to top](#migration-assessment-report)]

# Assessment Widgets
Assessment widgets query tables in the $inventory database and summarize or detail out findings.

The second row of the report starts with "Job Count", "Readiness", "Assessment Summary", "Table counts by storage" and "Table counts by schema and format"

<img width="1510" alt="image" src="https://github.com/databrickslabs/ucx/assets/106815134/b7ea36a6-165b-4172-933d-b0b049707316">

[[back to top](#migration-assessment-report)]

## Readiness

This is a rough summary of the workspace readiness to run Unity Catalog governed workloads. Each line item is the percent of compatible items divided by the total items in the class.

[[back to top](#migration-assessment-report)]

## Assessment Summary

This is a summary count, per finding type of all of the findings identified during the assessment workflow. The assessment summary will help identify areas that need focus (e.g. Tables on DBFS or Clusters that need DBR upgrades)

[[back to top](#migration-assessment-report)]

## Table counts by storage

This is a summary count of Hive Metastore tables, per storage type (DBFS Root, DBFS Mount, Cloud Storage (referred as External)). This also gives a summary count of tables using storage types which are unsupported (such as WASB or ADL in Azure) in Unity Catalog. Count of tables created using Databricks Demo Datasets are also identified here

[[back to top](#migration-assessment-report)]

## Table counts by schema and format

This is a summary count by Hive Metastore (HMS) table formats (Delta and Non Delta) for each HMS schema    

The third row continues with "Database Summary"
<img width="1220" alt="image" src="https://github.com/databrickslabs/ucx/assets/1122251/28742e33-d3e3-4eb8-832f-1edd34999fa2">

[[back to top](#migration-assessment-report)]

## Database Summary

This is a Hive Metastore based Database by Database assessment summary along with an upgrade strategy.
`In Place Sync` indicates that the `SYNC` command can be used to copy the metadata into a Unity Catalog Catalog.

And the fourth row contains "External Locations" and "Mount Points"
<img width="1231" alt="image" src="https://github.com/databrickslabs/ucx/assets/1122251/8a88da36-43ef-4f50-8818-6bc7e4e23758">

[[back to top](#migration-assessment-report)]

## External Locations

Tables were scanned for `LOCATION` attributes and that list was distilled down to External Locations. In Unity Catalog, create a STORAGE CREDENTIAL that can access the External Locations, then define Unity Catalog `EXTERNAL LOCATION`s for these items.

[[back to top](#migration-assessment-report)]

## Mount Points

Mount points are popular means to provide access to external buckets / storage accounts. A more secure form in Unity Catalog are EXTERNAL LOCATIONs and VOLUMES. EXTERNAL LOCATIONs are the basis for EXTERNAL Tables, Schemas, Catalogs and VOLUMES. VOLUMES are the basis for managing files. 
The recommendation is to migrate Mountpoints to Either EXTERNAL LOCATIONS or VOLUMEs. The Unity Catalog Create External Location UI will prompt for mount points to assist in creating EXTERNAL LOCATIONS.

Unfortunately, as of January 2024, cross cloud external locations are not supported. Databricks to Databricks delta sharing may assist in upgrading cross cloud mounts.

The next row contains the "Table Types" widget
<img width="1229" alt="image" src="https://github.com/databrickslabs/ucx/assets/1122251/859d7ea1-5f73-4278-9748-80ca6d94fe28">

[[back to top](#migration-assessment-report)]

## Table Types

This widget is a detailed list of each table, it's format, storage type, location property and if a DBFS table approximate table size. Upgrade strategies include:
- DEEP CLONE or CTAS for DBFS ROOT tables
- SYNC for DELTA tables (managed or external) for tables stored on a non-DBFS root (Mount point or direct cloud storage path)
- Managed non DELTA tables need to be upgraded to to Unity Catalog by either:
   - Use CTAS to convert targeting the Unity Catalog catalog, schema and table name
   - Moved to an EXTERNAL LOCATION and create an EXTERNAL table in Unity Catalog.

The following row includes "Incompatible Clusters and "Incompatible Jobs"
<img width="1248" alt="image" src="https://github.com/databrickslabs/ucx/assets/1122251/30a08de6-240c-48d1-9f49-e2c10537ccc3">

[[back to top](#migration-assessment-report)]

## Incompatible Clusters

This widget is a list of findings (reasons) and clusters that may need upgrading. See Assessment Finding Index (below) for specific recommendations.

[[back to top](#migration-assessment-report)]

## Incompatible Jobs

This is a list of findings (reasons) and jobs that may need upgrading. See Assessment Findings Index for more information.

The final row includes "Incompatible Delta Live Tables" and "Incompatible Global Init Scripts"
<img width="1244" alt="image" src="https://github.com/databrickslabs/ucx/assets/1122251/c0267df9-ddb1-4519-8ba1-4c608d8eef31">

[[back to top](#migration-assessment-report)]

## Incompatible Delta Live Tables

These are Delta Live Table jobs that may be incompatible with Unity Catalog.

[[back to top](#migration-assessment-report)]

## Incompatible Global Init Scripts

These are Global Init Scripts that are incompatible with Unity Catalog compute. As a reminder, global init scripts need to be on secure storage (Volumes or a Cloud Storage account and not DBFS)

[[back to top](#migration-assessment-report)]

# Assessment Finding Index

This section will help explain UCX Assessment findings and provide a recommended action.
The assessment finding index is grouped by:
- The 100 series findings are Databricks Runtime and compute configuration findings.
- The 200 series findings are centered around data related observations.

[[back to top](#migration-assessment-report)]

### AF101 - not supported DBR: ##.#.x-scala2.12

Short description: The compute runtime does not meet the requirements to use Unity Catalog.
Explanation: Unity Catalog capabilities are fully enabled on Databricks Runtime 13.3 LTS. This is the current recommended runtime for production interactive clusters and jobs. This finding is noting the cluster or job compute configuration does not meet this threshold. 
recommendation: Upgrade the DBR version to 13.3 LTS or later.

[[back to top](#migration-assessment-report)]

### AF102 - not supported DBR: ##.#.x-cpu-ml-scala2.12

Currently, MLR (Machine Learning Runtime) and GPU *SHARED* clusters are not supported with Unity Catalog. Use *Assigned* or *Job* clusters instead.

[[back to top](#migration-assessment-report)]

### AF103 - not supported DBR: ##.#.x-gpu-ml-scala2.12

Currently, MLR (Machine Learning Runtime) and GPU *SHARED* clusters are not supported with Unity Catalog. Use *Assigned* or *Job* clusters instead.

[[back to top](#migration-assessment-report)]

### AF111 - Uses azure service principal credentials config in cluster.

Azure service principles are replaced by Storage Credentials to access cloud storage accounts.
Create a storage CREDENTIAL, then an EXTERNAL LOCATION and possibly external tables to provide data access.
If the service principal is used to access additional azure cloud services, convert the cluster to a `Assigned` cluster type which *may* work.

[[back to top](#migration-assessment-report)]

### AF112 - Uses azure service principal credentials config in Job cluster.

Azure service principles are replaced by Storage Credentials to access cloud storage accounts.
Create a storage CREDENTIAL, then an EXTERNAL LOCATION and possibly external tables to provide data access.
If the service principal is used to access additional azure cloud services, convert the job cluster to a `Assigned` cluster type which *may* work.

[[back to top](#migration-assessment-report)]

### AF113 - Uses azure service principal credentials config in pipeline.

Azure service principles are replaced by Storage Credentials to access cloud storage accounts.
Create a storage CREDENTIAL, then an EXTERNAL LOCATION and possibly external tables to provide data access.

[[back to top](#migration-assessment-report)]

### AF114 - unsupported config

A spark config option was found in a cluster compute definition that is incompatible with Unity Catalog based compute. The recommendation is to remove or alter the config. Additionally, Unity Catalog enabled clusters may require a different approach to the same capability. As a transition strategy, "Unassigned" clusters or "Assigned" (including job clusters but not shared clusters) may work.
- `spark.hadoop.javax.jdo.option.ConnectionURL` an external Hive Metastore is in use. Recommend migrating the these tables and schemas to Unity Catalog external tables where they can be shared across workspaces.

[[back to top](#migration-assessment-report)]

### AF115 - unsupported config: spark.databricks.passthrough.enabled

Passthrough security model is not supported by Unity Catalog. Passthrough mode relied upon file based authorization which is incompatible with Fine Grained Access Controls supported by Unity Catalog.
Recommend mapping your Passthrough security model to a External Location/Volume/Table/View based security model compatible with Unity Catalog.

[[back to top](#migration-assessment-report)]

### AF116 - No isolation shared clusters not supported in UC

Unity Catalog data cannot be accessed from No Isolation clusters, they should not be used.

[[back to top](#migration-assessment-report)]

### AF117 - cluster type not supported

Only Assigned and Shared access mode are supported in UC.
You must change your cluster configuration to match UC compliant access modes.

[[back to top](#migration-assessment-report)]

### AF201 - Inplace Sync

Short description: We found that the table or database can be SYNC'd without moving data because the data is stored directly on cloud storage specified via a mount or a cloud storage URL (not DBFS).
How: Run the SYNC command on the table or schema.  If the tables (or source database) is 'managed' first set this spark setting in your session or in the interactive cluster configuration: `spark.databricks.sync.command.enableManagedTable=true`

[[back to top](#migration-assessment-report)]

### AF202 - Asset Replication Required

We found that the table or database needs to have the data copied into a Unity Catalog managed location or table.
Recommendation: Perform a 'deep clone' operation on the table to copy the files 
```sql
CREATE TABLE [IF NOT EXISTS] table_name
   [SHALLOW | DEEP] CLONE source_table_name [TBLPROPERTIES clause] [LOCATION path]   
```

[[back to top](#migration-assessment-report)]

### AF203 - Data in DBFS Root

A table or schema refers to a location in DBFS and not a cloud storage location.
The data must be moved from DBFS to a cloud storage location or to a Unity Catalog managed storage.

[[back to top](#migration-assessment-report)]

### AF204 - Data is in DBFS Mount

A table or schema refers to a location in DBFS mount and not a direct cloud storage location.
Mounts are not suppored in Unity Catalog so the mount source location must be de-referenced and the table/schema objects mapped to a UC external location.

[[back to top](#migration-assessment-report)]

### AF210 - Non-DELTA format: CSV

Unity Catalog does not support managed CSV tables. Recommend converting the table to DELTA format or migrating the table to an External table.

[[back to top](#migration-assessment-report)]

### AF211 - Non-DELTA format: DELTA

This was a known [issue](https://github.com/databrickslabs/ucx/issues/788) of the UCX assessment job. This bug should be fixed with release `0.10.0`

[[back to top](#migration-assessment-report)]

### AF212 - Non-DELTA format

where format can be any of `[PARQUET|JDBC|ORC|XML|JSON|HIVE|deltaSharing|com.databricks.spark.csv|...]`

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

[[back to top](#migration-assessment-report)]

### AF221 - Unsupported Storage Type

where storage type can be any of `adl://`, `wasb://`, or `wasbs://`.

ADLS Gen 2 (`abfss://`) is the only Azure native storage type supported. Use a Deep Clone process to copy the table data.
```sql
CREATE TABLE [IF NOT EXISTS] table_name
   [SHALLOW | DEEP] CLONE source_table_name [TBLPROPERTIES clause] [LOCATION path]   
```

[[back to top](#migration-assessment-report)]

# Common Terms

## UC

Abbreviation for Unity Catalog

## DELTA

DELTA refers to the table format for Delta Lake tables.
## CTAS

Abbreviation for *Create Table As Select* which is a method of copying table data from one source to another. The CREATE statement can include USING and LOCATION keywords while the SELECT portion can cast columns to other data types.

## DEEP CLONE

Is shortcut for CREATE TABLE DEEP CLONE <target table> <source table> which only works for DELTA formatted tables.

## EXTERNAL LOCATION

[EXTERNAL LOCATION]([url](https://docs.databricks.com/en/connect/unity-catalog/external-locations.html#create-an-external-location)) is a UC object type describing a url to a cloud storage bucket + folder or storage account + container and folder

## STORAGE CREDENTIAL

[STORAGE CREDENTIAL]([url](https://docs.databricks.com/en/sql/language-manual/sql-ref-storage-credentials.html)https://docs.databricks.com/en/sql/language-manual/sql-ref-storage-credentials.html) are a UC object encapsulating the credentials necessary to access cloud storage.

[[back to top](#migration-assessment-report)]

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
  * [AF300 - AF399](#af300---af399)
    * [AF300.6 - 3 level namespace](#af3006---3-level-namespace)
    * [AF300.1 - r language support](#af3001---r-language-support)
    * [AF300.2 - scala language support](#af3002---scala-language-support)
    * [AF300.3 - Minimum DBR version](#af3003---minimum-dbr-version)
    * [AF300.4 - ML Runtime cpu](#af3004---ml-runtime-cpu)
    * [AF300.5 - ML Runtime gpu](#af3005---ml-runtime-gpu)
    * [AF301.1 - spark.catalog.x](#af3011---sparkcatalogx)
    * [AF301.2 - spark.catalog.x (spark._jsparkSession.catalog)](#af3012---sparkcatalogx-spark_jsparksessioncatalog)
  * [AF302.x - Arbitrary Java](#af302x---arbitrary-java)
    * [AF302.1 - Arbitrary Java (`spark._jspark`)](#af3021---arbitrary-java-spark_jspark)
    * [AF302.2 - Arbitrary Java (`spark._jvm`)](#af3022---arbitrary-java-spark_jvm)
    * [AF302.3 - Arbitrary Java (`._jdf`)](#af3023---arbitrary-java-_jdf)
    * [AF302.4 - Arbitrary Java (`._jcol`)](#af3024---arbitrary-java-_jcol)
    * [AF302.5 - Arbitrary Java (`._jvm`)](#af3025---arbitrary-java-_jvm)
    * [AF302.6 - Arbitrary Java (`._jvm.org.apache.log4j`)](#af3026---arbitrary-java-_jvmorgapachelog4j)
    * [AF303.1 - Java UDF (`spark.udf.registerJavaFunction`)](#af3031---java-udf-sparkudfregisterjavafunction)
    * [AF304.1 - JDBC datasource (`spark.read.format("jdbc")`)](#af3041---jdbc-datasource-sparkreadformatjdbc)
    * [AF305.1 - boto3](#af3051---boto3)
    * [AF305.2 - s3fs](#af3052---s3fs)
    * [AF306.1 - dbutils...getContext (`.toJson()`)](#af3061---dbutilsgetcontext-tojson)
    * [AF306.2 - dbutils...getContext](#af3062---dbutilsgetcontext)
    * [AF310.1 - credential passthrough (`dbutils.credentials.`)](#af3101---credential-passthrough-dbutilscredentials)
  * [AF311.x - dbutils (`dbutils`)](#af311x---dbutils-dbutils)
    * [AF311.1 - dbutils.fs (`dbutils.fs.`)](#af3111---dbutilsfs-dbutilsfs)
    * [AF311.2 - dbutils mount(s) (`dbutils.fs.mount`)](#af3112---dbutils-mounts-dbutilsfsmount)
    * [AF311.3 - dbutils mount(s) (`dbutils.fs.refreshMounts`)](#af3113---dbutils-mounts-dbutilsfsrefreshmounts)
    * [AF311.4 - dbutils mount(s) (`dbutils.fs.unmount`)](#af3114---dbutils-mounts-dbutilsfsunmount)
    * [AF311.5 - mount points (`dbfs:/mnt`)](#af3115---mount-points-dbfsmnt)
    * [AF311.6 - dbfs usage (`dbfs:/`)](#af3116---dbfs-usage-dbfs)
    * [AF311.7 - dbfs usage (`/dbfs/`)](#af3117---dbfs-usage-dbfs)
  * [AF313.x - SparkContext](#af313x---sparkcontext)
    * [AF313.1 - SparkContext (`spark.sparkContext`)](#af3131---sparkcontext-sparksparkcontext)
    * [AF313.2 - SparkContext (`from pyspark.sql import SQLContext`)](#af3132---sparkcontext-from-pysparksql-import-sqlcontext)
    * [AF313.3 - SparkContext (`.binaryFiles`)](#af3133---sparkcontext-binaryfiles)
    * [AF313.4 - SparkContext (`.binaryRecords`)](#af3134---sparkcontext-binaryrecords)
    * [AF313.5 - SparkContext (`.emptyRDD`)](#af3135---sparkcontext-emptyrdd)
    * [AF313.6 - SparkContext (`.getConf`)](#af3136---sparkcontext-getconf)
    * [AF313.7 - SparkContext ( `.hadoopFile` )](#af3137---sparkcontext--hadoopfile-)
    * [AF313.8 - SparkContext ( `.hadoopRDD` )](#af3138---sparkcontext--hadooprdd-)
    * [AF313.9 - SparkContext ( `.init_batched_serializer` )](#af3139---sparkcontext--init_batched_serializer-)
    * [AF313.10 - SparkContext ( `.newAPIHadoopFile` )](#af31310---sparkcontext--newapihadoopfile-)
    * [AF313.11 - SparkContext ( `.newAPIHadoopRDD` )](#af31311---sparkcontext--newapihadooprdd-)
    * [AF313.12 - SparkContext ( `.parallelize` )](#af31312---sparkcontext--parallelize-)
    * [AF313.13 - SparkContext ( `.pickleFile` )](#af31313---sparkcontext--picklefile-)
    * [AF313.14 - SparkContext ( `.range` )](#af31314---sparkcontext--range-)
    * [AF313.15 - SparkContext ( `.rdd` )](#af31315---sparkcontext--rdd-)
    * [AF313.16 - SparkContext ( `.runJob` )](#af31316---sparkcontext--runjob-)
    * [AF313.17 - SparkContext ( `.sequenceFile` )](#af31317---sparkcontext--sequencefile-)
    * [AF313.18 - SparkContext ( `.setJobGroup` )](#af31318---sparkcontext--setjobgroup-)
    * [AF313.19 - SparkContext ( `.setLocalProperty` )](#af31319---sparkcontext--setlocalproperty-)
    * [AF313.20 - SparkContext ( `.setSystemProperty` )](#af31320---sparkcontext--setsystemproperty-)
    * [AF313.21 - SparkContext ( `.stop` )](#af31321---sparkcontext--stop-)
    * [AF313.22 - SparkContext ( `.textFile` )](#af31322---sparkcontext--textfile-)
    * [AF313.23 - SparkContext ( `.uiWebUrl`)](#af31323---sparkcontext--uiweburl)
    * [AF313.24 - SparkContext (`.union`)](#af31324---sparkcontext-union)
    * [AF313.25 - SparkContext (`.wholeTextFiles`)](#af31325---sparkcontext-wholetextfiles)
  * [AF314.x - Distributed ML](#af314x---distributed-ml)
    * [AF314.1 - Distributed ML (`sparknlp`)](#af3141---distributed-ml-sparknlp)
    * [AF314.2 - Distributed ML (`xgboost.spark`)](#af3142---distributed-ml-xgboostspark)
    * [AF314.3 - Distributed ML (`catboost_spark`)](#af3143---distributed-ml-catboost_spark)
    * [AF314.4 - Distributed ML (`ai.catboost:catboost-spark`)](#af3144---distributed-ml-aicatboostcatboost-spark)
    * [AF314.5 - Distributed ML (`hyperopt`)](#af3145---distributed-ml-hyperopt)
    * [AF314.6 - Distributed ML (`SparkTrials`)](#af3146---distributed-ml-sparktrials)
    * [AF314.7 - Distributed ML (`horovod.spark`)](#af3147---distributed-ml-horovodspark)
    * [AF314.8 - Distributed ML (`ray.util.spark`)](#af3148---distributed-ml-rayutilspark)
    * [AF314.9 - Distributed ML (`databricks.automl`)](#af3149---distributed-ml-databricksautoml)
    * [AF308.1 - Graphframes (`from graphframes`)](#af3081---graphframes-from-graphframes)
    * [AF309.1 - Spark ML (`pyspark.ml.`)](#af3091---spark-ml-pysparkml)
    * [AF315.1 - UDAF scala issue (`UserDefinedAggregateFunction`)](#af3151---udaf-scala-issue-userdefinedaggregatefunction)
    * [AF315.2 - applyInPandas (`applyInPandas`)](#af3152---applyinpandas-applyinpandas)
    * [AF315.3 - mapInPandas (`mapInPandas`)](#af3153---mapinpandas-mapinpandas)
  * [AF330.x - Streaming](#af330x---streaming)
    * [AF330.1 - Streaming (`.trigger(continuous`)](#af3301---streaming-triggercontinuous)
    * [AF330.2 - Streaming (`kafka.sasl.client.callback.handler.class`)](#af3302---streaming-kafkasaslclientcallbackhandlerclass)
    * [AF330.3 - Streaming (`kafka.sasl.login.callback.handler.class`)](#af3303---streaming-kafkasasllogincallbackhandlerclass)
    * [AF330.4 - Streaming (`kafka.sasl.login.class`)](#af3304---streaming-kafkasaslloginclass)
    * [AF330.5 - Streaming (`kafka.partition.assignment.strategy`)](#af3305---streaming-kafkapartitionassignmentstrategy)
    * [AF330.6 - Streaming (`kafka.ssl.truststore.location`)](#af3306---streaming-kafkassltruststorelocation)
    * [AF330.7 - Streaming (`kafka.ssl.keystore.location`)](#af3307---streaming-kafkasslkeystorelocation)
    * [AF330.8 - Streaming (`cleanSource`)](#af3308---streaming-cleansource)
    * [AF330.9 - Streaming (`sourceArchiveDir`)](#af3309---streaming-sourcearchivedir)
    * [AF330.10 - Streaming (`applyInPandasWithState`)](#af33010---streaming-applyinpandaswithstate)
    * [AF330.11 - Streaming (`.format("socket")`)](#af33011---streaming-formatsocket)
    * [AF330.12 - Streaming (`StreamingQueryListener`)](#af33012---streaming-streamingquerylistener)
    * [AF330.13 - Streaming (`applyInPandasWithState`)](#af33013---streaming-applyinpandaswithstate)
* [Common Terms](#common-terms)
  * [UC](#uc)
  * [DELTA](#delta)
  * [CTAS](#ctas)
  * [DEEP CLONE](#deep-clone)
  * [EXTERNAL LOCATION](#external-location)
  * [STORAGE CREDENTIAL](#storage-credential)
  * [Assigned Clusters or Single User Clusters](#assigned-clusters-or-single-user-clusters)
  * [Shared Clusters](#shared-clusters)
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
- The 300 series findings relate to [Compute Access mode limitations for Unity Catalog](https://docs.databricks.com/en/compute/access-mode-limitations.html#spark-api-limitations-for-unity-catalog-shared-access-mode)

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

### AF114 - Uses external Hive metastore config: spark.hadoop.javax.jdo.option.ConnectionURL

Spark configurations for External Hive metastore was found in a cluster definition. Unity Catalog is the recommended approach
for sharing data across workspaces. The recommendation is to remove the config after migrating the existing tables & views
using UCX. As a transition strategy, "No Isolation Shared" clusters or "Assigned" clusters will work.
- `spark.hadoop.javax.jdo.option.ConnectionURL` an external Hive Metastore is in use. Recommend migrating the these tables and schemas to Unity Catalog external tables where they can be shared across workspaces.
- `spark.databricks.hive.metastore.glueCatalog.enabled` Glue is used as external Hive Metastore. Recommend migrating the these tables and schemas to Unity Catalog external tables where they can be shared across workspaces.

[[back to top](#migration-assessment-report)]

### AF115 - Uses passthrough config: spark.databricks.passthrough.enabled.

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

## AF300 - AF399
The 300 series findings relate to [Compute Access mode limitations for Unity Catalog](https://docs.databricks.com/en/compute/access-mode-limitations.html#spark-api-limitations-for-unity-catalog-shared-access-mode)

Resolutions may include:

-  Upgrade cluter runtime to latest LTS version (e.g. 14.3 LTS)

-  Migrate users with GPU, Distributed ML, Spark Context, R type workloads to Assigned clusters. Implement cluster policies and pools to even out startup time and limit upper cost boundry.

-  Upgrade SQL only users (and BI tools) to SQL Warehouses (much better SQL / Warehouse experience and lower cost)

-  For users with single node python ML requirements, Shared Compute with `%pip install` library support or Personal Compute with pools and compute controls may provide a better experience and better manageability.

- For single node ML users on a crowded driver node of a large shared cluster, will get a better experience with Personal Compute policies combined with (warm) Compute pools

### AF300.6 - 3 level namespace

The `hive_metastore.` is used to refer to a 3 level namespace. Most customers will migrate hive_metastore to a workspace catalog (named based on the workspace name). The code should then map the `hive_metastore` to the appropriate catalog name.

Easier solution is to define a default catalog for your session, job, cluster or workspace.

Setting the Workspace default, use the admin UI or command line:
```
% databricks settings update-default-workspace-namespace
```

Setting the Cluster or Job default catalog (in the spark configuration settings):
```
spark.databricks.sql.initial.catalog.name my_catalog
```

For JDBC, add to the JDBC connection URL:
- `ConnCatalog=my_catalog`  (preferred)
- `databricks.Catalog=my_catalog` (rare)

For ODBC `.ini` file:
```
[Databricks]
Driver=<path-to-driver>
Host=<server-hostname>
Port=443
HTTPPath=<http-path>
ThriftTransport=2
SSL=1
AuthMech=3
UID=token
PWD=<personal-access-token>
Catalog=my_catalog
```

[[back to top](#migration-assessment-report)]

### AF300.1 - r language support
When using `%r` command cells, the user will receive `Your administrator has only allowed sql and python and scala commands on this cluster. This execution contained at least one disallowed language.` message.

Recommend using Assigned (single user clusters).

[[back to top](#migration-assessment-report)]

### AF300.2 - scala language support
Scala is supported on Databricks Runtime 13.3 and above.

Recommend upgrading your shared cluster DBR to 13.3 LTS or greater or using Assigned data security mode (single user clusters).

[[back to top](#migration-assessment-report)]

### AF300.3 - Minimum DBR version
The minimum DBR version to access Unity Catalog was not met. The recommendation is to upgrade to the latest Long Term Supported (LTS) version of the Databricks Runtime.

### AF300.4 - ML Runtime cpu
The Databricks ML Runtime is not supported on Shared Compute mode clusters. Recommend migrating these workloads to Assigned clusters. Implement cluster policies and pools to even out startup time and limit upper cost boundry.

### AF300.5 - ML Runtime gpu
The Databricks ML Runtime is not supported on Shared Compute mode clusters. Recommend migrating these workloads to Assigned clusters. Implement cluster policies and pools to even out startup time and limit upper cost boundry.

### AF301.1 - spark.catalog.x

The `spark.catalog.` pattern was found. Commonly used functions in spark.catalog, such as tableExists, listTables, setDefault catalog are not allowed/whitelisted on shared clusters due to security reasons. `spark.sql("<sql command>)` may be a better alternative. DBR 14.1 and above have made these commands available. Upgrade your DBR version.

[[back to top](#migration-assessment-report)]

### AF301.2 - spark.catalog.x (spark._jsparkSession.catalog)

The `spark._jsparkSession.catalog` pattern was found. Commonly used functions in spark.catalog, such as tableExists, listTables, setDefault catalog are not allowed/whitelisted on shared clusters due to security reasons. `spark.sql("<sql command>)` may be a better alternative. The corresponding `spark.catalog.x` methods may work on DBR 14.1 and above.

[[back to top](#migration-assessment-report)]

## AF302.x - Arbitrary Java
With Spark Connect on Shared clusters it is no longer possible to directly access the host JVM from the Python process. This means it is no longer possible to interact with Java classes or instantiate arbitrary Java classes directly from Python similar to the code below.

Recommend finding the equivalent PySpark or Scala api.

### AF302.1 - Arbitrary Java (`spark._jspark`)

The `spark._jspark` is used to execute arbitrary Java code.

[[back to top](#migration-assessment-report)]

### AF302.2 - Arbitrary Java (`spark._jvm`)

The `spark._jvm` is used to execute arbitrary Java code.

[[back to top](#migration-assessment-report)]

### AF302.3 - Arbitrary Java (`._jdf`)

The `._jdf` is used to execute arbitrary Java code.

[[back to top](#migration-assessment-report)]

### AF302.4 - Arbitrary Java (`._jcol`)

The `._jcol` is used to execute arbitrary Java code.

[[back to top](#migration-assessment-report)]

### AF302.5 - Arbitrary Java (`._jvm`)

The `._jvm` is used to execute arbitrary Java code.

[[back to top](#migration-assessment-report)]

### AF302.6 - Arbitrary Java (`._jvm.org.apache.log4j`)

The `._jvm.org.apache.log4j` is used to execute arbitrary Java code.

[[back to top](#migration-assessment-report)]

### AF303.1 - Java UDF (`spark.udf.registerJavaFunction`)

The `spark.udf.registerJavaFunction` is used to register a Java UDF.

[[back to top](#migration-assessment-report)]

### AF304.1 - JDBC datasource (`spark.read.format("jdbc")`)

The `spark.read.format("jdbc")` pattern was found and is used to read data from a JDBC datasource.

Accessing third-party databases—other than MySQL, PostgreSQL, Amazon Redshift, Snowflake, Microsoft SQL Server, Azure Synapse (SQL Data Warehouse) and Google BigQuery will require additional permissions on a shared cluster if the user is not a workspace admin. This is due to the drivers not guaranteeing user isolation, e.g., as the driver writes data from multiple users to a widely accessible temp directory.

Workaround:
Granting ANY FILE permissions will allow users to access untrusted databases. Note that ANY FILE will still enforce ACLs on any tables or external (storage) locations governed by Unity Catalog.
This requires DBR 12.2 or later (DBR 12.1 or before is blocked on the network layer)

[[back to top](#migration-assessment-report)]

### AF305.1 - boto3

The `boto3` library is used.

Instance profiles (AWS) are not supported from the Python/Scala REPL or UDFs, e.g. using boto3 or s3fs, Instance profiles are only set from init scripts and (internally) from Spark. To access S3 objects the recommendation is to use EXTERNAL VOLUMES mapped to the fixed s3 storage location.

**Workarounds**
For accessing cloud storage (S3), use storage credentials and external locations. 

(AWS) Consider other ways to authenticate with boto3, e.g., by passing credentials from Databricks secrets directly to boto3 as a parameter, or loading them as environment variables. This page contains more information. Please note that unlike instance profiles, those methods do not provide short-lived credentials out of the box, and customers are responsible for rotating secrets according to their security needs.

[[back to top](#migration-assessment-report)]

### AF305.2 - s3fs

The `s3fs` library is used which provides posix type sematics for S3 access. s3fs is based on boto3 library and has similar restrictions. The recommendation is to use EXTERNAL VOLUMES mapped to the fixed s3 storage location or MANAGED VOLUMES.

[[back to top](#migration-assessment-report)]

### AF306.1 - dbutils...getContext (`.toJson()`)

The `dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()` pattern was found. This function may trigger a security exception in DBR 13.0 and above.

The recommendation is to explore alternative APIs:
```
from dbruntime.databricks_repl_context import get_context
context = get_context()
context.__dict__
```

[[back to top](#migration-assessment-report)]

### AF306.2 - dbutils...getContext

The `dbutils.notebook.entry_point.getDbutils().notebook().getContext()` pattern was found. This function may trigger a security exception in DBR 13.0 and above.

```
from dbruntime.databricks_repl_context import get_context
context = get_context()
context.__dict__
```

[[back to top](#migration-assessment-report)]

### AF310.1 - credential passthrough (`dbutils.credentials.`)

The `dbutils.credentials.` is used for credential passthrough. This is not supported by Unity Catalog.

[[back to top](#migration-assessment-report)]

## AF311.x - dbutils (`dbutils`)

DBUtils and other clients that directly read the data from cloud storage are not supported. 
Use [Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html) or use Assigned clusters.

### AF311.1 - dbutils.fs (`dbutils.fs.`)

The `dbutils.fs.` pattern was found. DBUtils and other clients that directly read the data from cloud storage are not supported. Please note that `dbutils.fs` calls with /Volumes ([Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)) will work.

[[back to top](#migration-assessment-report)]

### AF311.2 - dbutils mount(s) (`dbutils.fs.mount`)

The `dbutils.fs.mount` pattern was found. This is not supported by Unity Catalog. Use instead EXTERNAL LOCATIONS and VOLUMES.

[[back to top](#migration-assessment-report)]

### AF311.3 - dbutils mount(s) (`dbutils.fs.refreshMounts`)

The `dbutils.fs.refreshMounts` pattern was found. This is not supported by Unity Catalog. Use instead EXTERNAL LOCATIONS and VOLUMES.

[[back to top](#migration-assessment-report)]

### AF311.4 - dbutils mount(s) (`dbutils.fs.unmount`)

The `dbutils.fs.unmount` pattern was found. This is not supported by Unity Catalog. Use instead EXTERNAL LOCATIONS and VOLUMES.

[[back to top](#migration-assessment-report)]

### AF311.5 - mount points (`dbfs:/mnt`)

The `dbfs:/mnt` is used as a mount point. This is not supported by Unity Catalog. Use instead EXTERNAL LOCATIONS and VOLUMES.

[[back to top](#migration-assessment-report)]

### AF311.6 - dbfs usage (`dbfs:/`)

The `dbfs:/` pattern was found. DBFS is not supported by Unity Catalog. Use instead EXTERNAL LOCATIONS and VOLUMES. There may be false positives with this pattern because `dbfs:/Volumes/mycatalog/myschema/myvolume` is ligitamate usage.

Please Note: `dbfs:/Volumes/<catalog>/<schema>/<volume>` is a supported access pattern for spark.

[[back to top](#migration-assessment-report)]

### AF311.7 - dbfs usage (`/dbfs/`)

The `/dbfs/` pattern was found. DBFS is not supported by Unity Catalog. Use instead EXTERNAL LOCATIONS and VOLUMES.

[[back to top](#migration-assessment-report)]

## AF313.x - SparkContext

Spark Context(sc), spark.sparkContext, and sqlContext are not supported for Scala in any Databricks Runtime and are not supported for Python in Databricks Runtime 14.0 and above with Shared Compute access mode due to security restrictions. In Shared Compute mode, these methods do not support strict data isolation.

To run legacy workloads without modification, use [access mode "Single User"](https://docs.databricks.com/en/compute/configure.html#access-modes) type clusters.

Databricks recommends using the spark variable to interact with the SparkSession instance.

The following sc functions are also not supported in "Shared" Access Mode: emptyRDD, range, init_batched_serializer, parallelize, pickleFile, textFile, wholeTextFiles, binaryFiles, binaryRecords, sequenceFile, newAPIHadoopFile, newAPIHadoopRDD, hadoopFile, hadoopRDD, union, runJob, setSystemProperty, uiWebUrl, stop, setJobGroup, setLocalProperty, getConf.

### AF313.1 - SparkContext (`spark.sparkContext`)

The `spark.sparkContext` pattern was found, use the `spark` variable directly.

[[back to top](#migration-assessment-report)]

### AF313.2 - SparkContext (`from pyspark.sql import SQLContext`)

The `from pyspark.sql import SQLContext` and `import org.apache.spark.sql.SQLContext` are used. These are not supported in Unity Catalog. Possibly, the `spark` variable will suffice.

[[back to top](#migration-assessment-report)]

### AF313.3 - SparkContext (`.binaryFiles`)

The `.binaryFiles` pattern was found, this is not supported by Unity Catalog. 
Instead, please consider using `spark.read.format('binaryFiles')`.

[[back to top](#migration-assessment-report)]

### AF313.4 - SparkContext (`.binaryRecords`)

The `.binaryRecords` pattern was found, which is not supported by Unity Catalog Shared Compute access mode.

[[back to top](#migration-assessment-report)]

### AF313.5 - SparkContext (`.emptyRDD`)

The `.emptyRDD` pattern was found, which is not supported by Unity Catalog Shared Compute access mode.
Instead use:
```python
%python
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([StructField("k", StringType(), True)])
spark.createDataFrame([], schema)
```

[[back to top](#migration-assessment-report)]

### AF313.6 - SparkContext (`.getConf`)

The `.getConf` pattern was found. There may be significant false positives with this one as `.getConf` is a common API pattern. In the case of `sparkContext.getConf` or `sc.getConf`, use `spark.conf` instead.

- spark.conf.get() # retrieves a single value
- spark.conf.set() # sets a single value (If allowed)
- spark.conf.getAll() unfortunately does not exist

[[back to top](#migration-assessment-report)]

### AF313.7 - SparkContext ( `.hadoopFile` )

The `.hadoopFile` pattern was found. Use "Assigned" access mode compute or make permanent file type changes.

[[back to top](#migration-assessment-report)]

### AF313.8 - SparkContext ( `.hadoopRDD` )

The `.hadoopRDD` pattern was found. Use "Assigned" access mode compute or make permanent file type changes.

[[back to top](#migration-assessment-report)]

### AF313.9 - SparkContext ( `.init_batched_serializer` )

The `.init_batched_serializer` pattern was found. No suggestions available at this time.

[[back to top](#migration-assessment-report)]

### AF313.10 - SparkContext ( `.newAPIHadoopFile` )

The `.newAPIHadoopFile` pattern was found. Use "Assigned" access mode compute or make permanent file type changes.

[[back to top](#migration-assessment-report)]

### AF313.11 - SparkContext ( `.newAPIHadoopRDD` )

The `.newAPIHadoopRDD` pattern was found. Use "Assigned" access mode compute or make permanent file type changes.

[[back to top](#migration-assessment-report)]

### AF313.12 - SparkContext ( `.parallelize` )

The `.parallelize` pattern was found. Instead of:
```python
json_content1 = "{'json_col1': 'hello', 'json_col2': 32}"
json_content2 = "{'json_col1': 'hello', 'json_col2': 'world'}"

json_list = []
json_list.append(json_content1)
json_list.append(json_content2)

df = spark.read.json(sc.parallelize(json_list))
display(df)
```
use:
```python
from pyspark.sql import Row
import json 
# Sample JSON data as a list of dictionaries (similar to JSON objects)
json_data_str = response.text
json_data = [json.loads(json_data_str)]

# Convert dictionaries to Row objects
rows = [Row(**json_dict) for json_dict in json_data]
# Create DataFrame from list of Row objects
df = spark.createDataFrame(rows)
# Show the DataFrame
df.display()
```

[[back to top](#migration-assessment-report)]

### AF313.13 - SparkContext ( `.pickleFile` )

The `.pickleFile` pattern was found. Use "Assigned" access mode compute or make permanent file type changes.

[[back to top](#migration-assessment-report)]

### AF313.14 - SparkContext ( `.range` )

The `.range` pattern was found. Use `spark.range()` instead of `sc.range()`

[[back to top](#migration-assessment-report)]

### AF313.15 - SparkContext ( `.rdd` )

The `.rdd` pattern was found. Use "Assigned" access mode compute or upgrade to the faster Spark DataFrame api.

[[back to top](#migration-assessment-report)]

### AF313.16 - SparkContext ( `.runJob` )

The `.runJob` pattern was found. Use "Assigned" access mode compute or upgrade to the faster Spark DataFrame api.

[[back to top](#migration-assessment-report)]

### AF313.17 - SparkContext ( `.sequenceFile` )

The `.sequenceFile` pattern was found. Use "Assigned" access mode compute or make permanent file type changes.

[[back to top](#migration-assessment-report)]

### AF313.18 - SparkContext ( `.setJobGroup` )

The `.setJobGroup` pattern was found.
`spark.addTag()` can attach a tag, and `getTags()` and `interruptTag(tag)` can be used to act upon the presence/absence of a tag. These APIs only work with Spark Connect (Shared Compute Mode) and will not work in “Assigned” access mode.

[[back to top](#migration-assessment-report)]

### AF313.19 - SparkContext ( `.setLocalProperty` )

The `.setLocalProperty` pattern was found.

[[back to top](#migration-assessment-report)]

### AF313.20 - SparkContext ( `.setSystemProperty` )

The `.setSystemProperty` pattern was found. Use "Assigned" access mode compute or find alternative within the spark API.

[[back to top](#migration-assessment-report)]

### AF313.21 - SparkContext ( `.stop` )

The `.stop` pattern was found. Use "Assigned" access mode compute or find alternative within the spark API.

[[back to top](#migration-assessment-report)]

### AF313.22 - SparkContext ( `.textFile` )

The `.textFile` pattern was found. Use "Assigned" access mode compute or find alternative within the spark API.

[[back to top](#migration-assessment-report)]

### AF313.23 - SparkContext ( `.uiWebUrl`)

The `.uiWebUrl` pattern was found. Use "Assigned" access mode compute or find alternative within the spark API.

[[back to top](#migration-assessment-report)]

### AF313.24 - SparkContext (`.union`)

The `.union` pattern was found, Use "Assigned" access mode compute or find alternative within the spark API.

[[back to top](#migration-assessment-report)]

### AF313.25 - SparkContext (`.wholeTextFiles`)

The `.wholeTextFiles` pattern was found. Use "Assigned" access mode compute or use the `spark.read().text("file_name")` API.

[[back to top](#migration-assessment-report)]

## AF314.x - Distributed ML
Databricks Runtime ML and Spark Machine Learning Library (MLlib) are not supported on shared Unity Catalog compute. The recommendation is to use Assigned mode cluster; Use cluster policies and (warm) compute pools to improve compute and cost management.

### AF314.1 - Distributed ML (`sparknlp`)

The `sparknlp` pattern was found. Use "Assigned" access mode compute.

[[back to top](#migration-assessment-report)]

### AF314.2 - Distributed ML (`xgboost.spark`)

The `xgboost.spark` pattern was found. Use "Assigned" access mode compute.

[[back to top](#migration-assessment-report)]

### AF314.3 - Distributed ML (`catboost_spark`)

The `catboost_spark` pattern was found. Use "Assigned" access mode compute.

[[back to top](#migration-assessment-report)]

### AF314.4 - Distributed ML (`ai.catboost:catboost-spark`)

The `ai.catboost:catboost-spark` pattern was found. Use "Assigned" access mode compute.

[[back to top](#migration-assessment-report)]

### AF314.5 - Distributed ML (`hyperopt`)

The `hyperopt` pattern was found. Use "Assigned" access mode compute.

[[back to top](#migration-assessment-report)]

### AF314.6 - Distributed ML (`SparkTrials`)

The `SparkTrials` pattern was found. Use "Assigned" access mode compute.

[[back to top](#migration-assessment-report)]

### AF314.7 - Distributed ML (`horovod.spark`)

The `horovod.spark` pattern was found. Use "Assigned" access mode compute.

[[back to top](#migration-assessment-report)]

### AF314.8 - Distributed ML (`ray.util.spark`)

The `ray.util.spark` pattern was found. Use "Assigned" access mode compute.

[[back to top](#migration-assessment-report)]

### AF314.9 - Distributed ML (`databricks.automl`)

The `databricks.automl` pattern was found. Use "Assigned" access mode compute.

[[back to top](#migration-assessment-report)]

### AF308.1 - Graphframes (`from graphframes`)

The `from graphframes` pattern was found. Use "Assigned" access mode compute.

[[back to top](#migration-assessment-report)]

### AF309.1 - Spark ML (`pyspark.ml.`)

The `pyspark.ml.` pattern was found. Use "Assigned" access mode compute.

[[back to top](#migration-assessment-report)]

### AF315.1 - UDAF scala issue (`UserDefinedAggregateFunction`)

The `UserDefinedAggregateFunction` pattern was found. Use "Assigned" access mode compute.

[[back to top](#migration-assessment-report)]

### AF315.2 - applyInPandas (`applyInPandas`)

The `applyInPandas` pattern was found. Use "Assigned" access mode compute.

[[back to top](#migration-assessment-report)]

### AF315.3 - mapInPandas (`mapInPandas`)

The `mapInPandas` pattern was found. Use "Assigned" access mode compute.

[[back to top](#migration-assessment-report)]


## AF330.x - Streaming
Streaming limitations for Unity Catalog shared access mode [documentation](https://docs.databricks.com/en/compute/access-mode-limitations.html#streaming-limitations-for-unity-catalog-shared-access-mode) should be consulted for more details. 

See also [Streaming limitations for Unity Catalog single user access mode](https://docs.databricks.com/en/compute/access-mode-limitations.html#streaming-single) and [Streaming limitations for Unity Catalog shared access mode](https://docs.databricks.com/en/compute/access-mode-limitations.html#streaming-shared).

The assessment patterns and specifics are as follows:

### AF330.1 - Streaming (`.trigger(continuous`)

The `.trigger(continuous` pattern was found. Continuous processing mode is not supported in Unity Catalog shared access mode.
Apache Spark continuous processing mode is not supported. See [Continuous Processing](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#continuous-processing) in the Spark Structured Streaming Programming Guide.

[[back to top](#migration-assessment-report)]

### AF330.2 - Streaming (`kafka.sasl.client.callback.handler.class`)

The `kafka.sasl.client.callback.handler.class` pattern was found. SASL features are not supported in Unity Catalog shared access mode.

[[back to top](#migration-assessment-report)]

### AF330.3 - Streaming (`kafka.sasl.login.callback.handler.class`)

The `kafka.sasl.login.callback.handler.class` pattern was found. SASL features are not supported in Unity Catalog shared access mode.

[[back to top](#migration-assessment-report)]

### AF330.4 - Streaming (`kafka.sasl.login.class`)

The `kafka.sasl.login.class` pattern was found. SASL features are not supported in Unity Catalog shared access mode.

[[back to top](#migration-assessment-report)]

### AF330.5 - Streaming (`kafka.partition.assignment.strategy`)

The `kafka.partition.assignment.strategy` pattern was found. Kafka features are not supported in Unity Catalog shared access mode.

[[back to top](#migration-assessment-report)]

### AF330.6 - Streaming (`kafka.ssl.truststore.location`)

The `kafka.ssl.truststore.location` pattern was found. SSL features are not supported in Unity Catalog shared access mode.

[[back to top](#migration-assessment-report)]

### AF330.7 - Streaming (`kafka.ssl.keystore.location`)

The `kafka.ssl.keystore.location` pattern was found. SSL features are not supported in Unity Catalog shared access mode.

[[back to top](#migration-assessment-report)]

### AF330.8 - Streaming (`cleanSource`)

The `cleanSource` pattern was found. The cleanSource operation is not supported in Unity Catalog shared access mode.

[[back to top](#migration-assessment-report)]

### AF330.9 - Streaming (`sourceArchiveDir`)

The `sourceArchiveDir` pattern was found. The sourceArchiveDir operation is not supported in Unity Catalog shared access mode.

[[back to top](#migration-assessment-report)]

### AF330.10 - Streaming (`applyInPandasWithState`)

The `applyInPandasWithState` pattern was found. The applyInPandasWithState operation is not supported in Unity Catalog shared access mode.

[[back to top](#migration-assessment-report)]

### AF330.11 - Streaming (`.format("socket")`)

The `.format("socket")` pattern was found. Socket source is not supported in Unity Catalog shared access mode.

[[back to top](#migration-assessment-report)]

### AF330.12 - Streaming (`StreamingQueryListener`)

The `StreamingQueryListener` pattern was found. StreamingQueryListener is not supported in Unity Catalog shared access mode.

[[back to top](#migration-assessment-report)]

### AF330.13 - Streaming (`applyInPandasWithState`)

The `applyInPandasWithState` pattern was found. The applyInPandasWithState operation is not supported in Unity Catalog shared access mode.

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

## Assigned Clusters or Single User Clusters
"Assigned Clusters" are Interactive clusters assigned to a single principal. Implicit in this term is that these clusters are enabled for Unity Catalog. Publically available today, "Assigned Clusters" can be assigned to a user and the user's identity is used to access data resources. The access to the cluster is restricted to that single user to ensure accountability and accuracy of the audit logs.

"Single User Clusters" are Interactive clusters that name one specific user account as user.

The `data_security_mode` for these clusters are `SINGLE_USER`

## Shared Clusters
"Shared Clusters are Interactive or Job clusters with an access mode of "SHARED".


The `data_security_mode` for these clusters are `USER_ISOLATION`.


[[back to top](#migration-assessment-report)]

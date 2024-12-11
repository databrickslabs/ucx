---
title: "Migration Assessment Report"
linkTitle: "Migration Assessment Report"
---


This document describes the Assessment Report generated from the UCX tools. The main assessment report includes dashlets, widgets and details of the assessment findings and common recommendations made based on the Assessment Finding (AF) Index entry.

![report](/images/assessment-report.png)

# Assessment Report Summary
The Assessment Report (Main) is the output of the Databricks Labs UCX assessment workflow. This report queries the $inventory database (e.g. `ucx`) and summarizes the findings of the assessment. The link to the Assessment Report (Main) can be found in your home folder, under `.ucx` in the README.py file. The user may also directly navigate to the Assessment report by clicking on `Dashboards` icon on the left to find the Dashboard.



# Assessment Widgets
<img width="1655" alt="image" src="https://github.com/databrickslabs/ucx/assets/1122251/808f7c68-fcc7-4caa-bab2-03f49a382256">



## Readiness
This is an overall summary of readiness detailed in the Readiness dashlet. This value is based on the ratio of findings divided by the total number of assets scanned.



## Total Databases
The total number of `hive_metastore` databases found during the assessment.



## Metastore Crawl Failures
Total number of failures encountered by the crawler while extracting metadata from the Hive Metastore and REST APIs.



## Total Tables
Total number of hive metastore tables discovered



## Storage Locations
Total number of identified storage locations based on scanning Hive Metastore tables and schemas



# Assessment Widgets
Assessment widgets query tables in the $inventory database and summarize or detail out findings.

The second row of the report starts with "Job Count", "Readiness", "Assessment Summary", "Table counts by storage" and "Table counts by schema and format"

<img width="1510" alt="image" src="https://github.com/databrickslabs/ucx/assets/106815134/b7ea36a6-165b-4172-933d-b0b049707316">



## Readiness

This is a rough summary of the workspace readiness to run Unity Catalog governed workloads. Each line item is the percent of compatible items divided by the total items in the class.



## Assessment Summary

This is a summary count, per finding type of all of the findings identified during the assessment workflow. The assessment summary will help identify areas that need focus (e.g. Tables on DBFS or Clusters that need DBR upgrades)



## Table counts by storage

This is a summary count of Hive Metastore tables, per storage type (DBFS Root, DBFS Mount, Cloud Storage (referred as External)). This also gives a summary count of tables using storage types which are unsupported (such as WASB or ADL in Azure) in Unity Catalog. Count of tables created using Databricks Demo Datasets are also identified here



## Table counts by schema and format

This is a summary count by Hive Metastore (HMS) table formats (Delta and Non Delta) for each HMS schema

The third row continues with "Database Summary"
<img width="1220" alt="image" src="https://github.com/databrickslabs/ucx/assets/1122251/28742e33-d3e3-4eb8-832f-1edd34999fa2">



## Database Summary

This is a Hive Metastore based Database by Database assessment summary along with an upgrade strategy.
`In Place Sync` indicates that the `SYNC` command can be used to copy the metadata into a Unity Catalog Catalog.

And the fourth row contains "External Locations" and "Mount Points"
<img width="1231" alt="image" src="https://github.com/databrickslabs/ucx/assets/1122251/8a88da36-43ef-4f50-8818-6bc7e4e23758">



## External Locations

Tables were scanned for `LOCATION` attributes and that list was distilled down to External Locations. In Unity Catalog, create a STORAGE CREDENTIAL that can access the External Locations, then define Unity Catalog `EXTERNAL LOCATION`s for these items.



## Mount Points

Mount points are popular means to provide access to external buckets / storage accounts. A more secure form in Unity Catalog are EXTERNAL LOCATIONs and VOLUMES. EXTERNAL LOCATIONs are the basis for EXTERNAL Tables, Schemas, Catalogs and VOLUMES. VOLUMES are the basis for managing files.
The recommendation is to migrate Mountpoints to Either EXTERNAL LOCATIONS or VOLUMEs. The Unity Catalog Create External Location UI will prompt for mount points to assist in creating EXTERNAL LOCATIONS.

Unfortunately, as of January 2024, cross cloud external locations are not supported. Databricks to Databricks delta sharing may assist in upgrading cross cloud mounts.

The next row contains the "Table Types" widget
<img width="1229" alt="image" src="https://github.com/databrickslabs/ucx/assets/1122251/859d7ea1-5f73-4278-9748-80ca6d94fe28">



## Table Types

This widget is a detailed list of each table, it's format, storage type, location property and if a DBFS table approximate table size. Upgrade strategies include:
- DEEP CLONE or CTAS for DBFS ROOT tables
- SYNC for DELTA tables (managed or external) for tables stored on a non-DBFS root (Mount point or direct cloud storage path)
- Managed non DELTA tables need to be upgraded to Unity Catalog by either:
   - Use CTAS to convert targeting the Unity Catalog catalog, schema and table name
   - Moved to an EXTERNAL LOCATION and create an EXTERNAL table in Unity Catalog.

The following row includes "Incompatible Clusters and "Incompatible Jobs"
<img width="1248" alt="image" src="https://github.com/databrickslabs/ucx/assets/1122251/30a08de6-240c-48d1-9f49-e2c10537ccc3">



## Incompatible Clusters

This widget is a list of findings (reasons) and clusters that may need upgrading. See Assessment Finding Index (below) for specific recommendations.



## Incompatible Jobs

This is a list of findings (reasons) and jobs that may need upgrading. See Assessment Findings Index for more information.

The final row includes "Incompatible Delta Live Tables" and "Incompatible Global Init Scripts"
<img width="1244" alt="image" src="https://github.com/databrickslabs/ucx/assets/1122251/c0267df9-ddb1-4519-8ba1-4c608d8eef31">



## Incompatible Object Privileges

These are permissions on objects that are not supported by Unit Catalog.



## Incompatible Delta Live Tables

These are Delta Live Table jobs that may be incompatible with Unity Catalog.



## Incompatible Global Init Scripts

These are Global Init Scripts that are incompatible with Unity Catalog compute. As a reminder, global init scripts need to be on secure storage (Volumes or a Cloud Storage account and not DBFS)



# Assessment Finding Index

This section will help explain UCX Assessment findings and provide a recommended action.
The assessment finding index is grouped by:
- The 100 series findings are Databricks Runtime and compute configuration findings.
- The 200 series findings are centered around data related observations.
- The 300 series findings relate to [Compute Access mode limitations for Unity Catalog](https://docs.databricks.com/en/compute/access-mode-limitations.html#spark-api-limitations-for-unity-catalog-shared-access-mode)



### AF101 - not supported DBR: ##.#.x-scala2.12

Short description: The compute runtime does not meet the requirements to use Unity Catalog.
Explanation: Unity Catalog capabilities are fully enabled on Databricks Runtime 13.3 LTS. This is the current recommended runtime for production interactive clusters and jobs. This finding is noting the cluster or job compute configuration does not meet this threshold.
recommendation: Upgrade the DBR version to 13.3 LTS or later.



### AF102 - not supported DBR: ##.#.x-cpu-ml-scala2.12

Currently, MLR (Machine Learning Runtime) and GPU *SHARED* clusters are not supported with Unity Catalog. Use *Assigned* or *Job* clusters instead.



### AF103 - not supported DBR: ##.#.x-gpu-ml-scala2.12

Currently, MLR (Machine Learning Runtime) and GPU *SHARED* clusters are not supported with Unity Catalog. Use *Assigned* or *Job* clusters instead.



### AF111 - Uses azure service principal credentials config in cluster.

Azure service principles are replaced by Storage Credentials to access cloud storage accounts.
Create a storage CREDENTIAL, then an EXTERNAL LOCATION and possibly external tables to provide data access.
If the service principal is used to access additional azure cloud services, convert the cluster to a `Assigned` cluster type which *may* work.



### AF112 - Uses azure service principal credentials config in Job cluster.

Azure service principles are replaced by Storage Credentials to access cloud storage accounts.
Create a storage CREDENTIAL, then an EXTERNAL LOCATION and possibly external tables to provide data access.
If the service principal is used to access additional azure cloud services, convert the job cluster to a `Assigned` cluster type which *may* work.



### AF113 - Uses azure service principal credentials config in pipeline.

Azure service principles are replaced by Storage Credentials to access cloud storage accounts.
Create a storage CREDENTIAL, then an EXTERNAL LOCATION and possibly external tables to provide data access.



### AF114 - Uses external Hive metastore config: spark.hadoop.javax.jdo.option.ConnectionURL

Spark configurations for External Hive metastore was found in a cluster definition. Unity Catalog is the recommended approach
for sharing data across workspaces. The recommendation is to remove the config after migrating the existing tables & views
using UCX. As a transition strategy, "No Isolation Shared" clusters or "Assigned" clusters will work.
- `spark.hadoop.javax.jdo.option.ConnectionURL` an external Hive Metastore is in use. Recommend migrating the these tables and schemas to Unity Catalog external tables where they can be shared across workspaces.
- `spark.databricks.hive.metastore.glueCatalog.enabled` Glue is used as external Hive Metastore. Recommend migrating the these tables and schemas to Unity Catalog external tables where they can be shared across workspaces.



### AF115 - Uses passthrough config: spark.databricks.passthrough.enabled.

Passthrough security model is not supported by Unity Catalog. Passthrough mode relied upon file based authorization which is incompatible with Fine Grained Access Controls supported by Unity Catalog.
Recommend mapping your Passthrough security model to an External Location/Volume/Table/View based security model compatible with Unity Catalog.



### AF116 - No isolation shared clusters not supported in UC

Unity Catalog data cannot be accessed from No Isolation clusters, they should not be used.



### AF117 - cluster type not supported

Only Assigned and Shared access mode are supported in UC.
You must change your cluster configuration to match UC compliant access modes.



### AF201 - Inplace Sync

Short description: We found that the table or database can be SYNC'd without moving data because the data is stored directly on cloud storage specified via a mount or a cloud storage URL (not DBFS).
How: Run the SYNC command on the table or schema.  If the tables (or source database) is 'managed' first set this spark setting in your session or in the interactive cluster configuration: `spark.databricks.sync.command.enableManagedTable=true`



### AF202 - Asset Replication Required

We found that the table or database needs to have the data copied into a Unity Catalog managed location or table.
Recommendation: Perform a 'deep clone' operation on the table to copy the files
```sql
CREATE TABLE [IF NOT EXISTS] table_name
   [SHALLOW | DEEP] CLONE source_table_name [TBLPROPERTIES clause] [LOCATION path]
```



### AF203 - Data in DBFS Root

A table or schema refers to a location in DBFS and not a cloud storage location.
The data must be moved from DBFS to a cloud storage location or to a Unity Catalog managed storage.



### AF204 - Data is in DBFS Mount

A table or schema refers to a location in DBFS mount and not a direct cloud storage location.
Mounts are not suppored in Unity Catalog so the mount source location must be de-referenced and the table/schema objects mapped to a UC external location.



### AF210 - Non-DELTA format: CSV

Unity Catalog does not support managed CSV tables. Recommend converting the table to DELTA format or migrating the table to an External table.



### AF211 - Non-DELTA format: DELTA

This was a known [issue](https://github.com/databrickslabs/ucx/issues/788) of the UCX assessment job. This bug should be fixed with release `0.10.0`



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



### AF221 - Unsupported Storage Type

where storage type can be any of `adl://`, `wasb://`, or `wasbs://`.

ADLS Gen 2 (`abfss://`) is the only Azure native storage type supported. Use a Deep Clone process to copy the table data.
```sql
CREATE TABLE [IF NOT EXISTS] table_name
   [SHALLOW | DEEP] CLONE source_table_name [TBLPROPERTIES clause] [LOCATION path]
```



### AF222 - Explicitly DENYing privileges is not supported in UC

Unity Catalog does not support `DENY` permissions on securable objects. These permissions will be updated during group migration, but won't be transferred during catalog migration.



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



### AF300.1 - r language support
When using `%r` command cells, the user will receive `Your administrator has only allowed sql and python and scala commands on this cluster. This execution contained at least one disallowed language.` message.

Recommend using Assigned (single user clusters).



### AF300.2 - scala language support
Scala is supported on Databricks Runtime 13.3 and above.

Recommend upgrading your shared cluster DBR to 13.3 LTS or greater or using Assigned data security mode (single user clusters).



### AF300.3 - Minimum DBR version
The minimum DBR version to access Unity Catalog was not met. The recommendation is to upgrade to the latest Long Term Supported (LTS) version of the Databricks Runtime.

### AF300.4 - ML Runtime cpu
The Databricks ML Runtime is not supported on Shared Compute mode clusters. Recommend migrating these workloads to Assigned clusters. Implement cluster policies and pools to even out startup time and limit upper cost boundary.

### AF300.5 - ML Runtime gpu
The Databricks ML Runtime is not supported on Shared Compute mode clusters. Recommend migrating these workloads to Assigned clusters. Implement cluster policies and pools to even out startup time and limit upper cost boundary.

### AF301.1 - spark.catalog.x

The `spark.catalog.` pattern was found. Commonly used functions in `spark.catalog`, such as `tableExists`, `listTables`, `setCurrentCatalog` are not allowed on shared clusters due to security reasons. `spark.sql("<sql command>)` may be a better alternative. DBR 14.1 and above have made these commands available. Upgrade your DBR version.



### AF301.2 - spark.catalog.x (spark._jsparkSession.catalog)

The `spark._jsparkSession.catalog` pattern was found. Commonly used functions in `spark.catalog`, such as `tableExists`, `listTables`, `setCurrentCatalog` are not allowed on shared clusters due to security reasons. `spark.sql("<sql command>)` may be a better alternative. The corresponding `spark.catalog.x` methods may work on DBR 14.1 and above.



## AF302.x - Arbitrary Java
With Spark Connect on Shared clusters it is no longer possible to directly access the host JVM from the Python process. This means it is no longer possible to interact with Java classes or instantiate arbitrary Java classes directly from Python similar to the code below.

Recommend finding the equivalent PySpark or Scala api.

### AF302.1 - Arbitrary Java (`spark._jspark`)

The `spark._jspark` is used to execute arbitrary Java code.



### AF302.2 - Arbitrary Java (`spark._jvm`)

The `spark._jvm` is used to execute arbitrary Java code.



### AF302.3 - Arbitrary Java (`._jdf`)

The `._jdf` is used to execute arbitrary Java code.



### AF302.4 - Arbitrary Java (`._jcol`)

The `._jcol` is used to execute arbitrary Java code.



### AF302.5 - Arbitrary Java (`._jvm`)

The `._jvm` is used to execute arbitrary Java code.



### AF302.6 - Arbitrary Java (`._jvm.org.apache.log4j`)

The `._jvm.org.apache.log4j` is used to execute arbitrary Java code.



### AF303.1 - Java UDF (`spark.udf.registerJavaFunction`)

The `spark.udf.registerJavaFunction` is used to register a Java UDF.



### AF304.1 - JDBC datasource (`spark.read.format("jdbc")`)

The `spark.read.format("jdbc")` pattern was found and is used to read data from a JDBC datasource.

Accessing third-party databases—other than MySQL, PostgreSQL, Amazon Redshift, Snowflake, Microsoft SQL Server, Azure Synapse (SQL Data Warehouse) and Google BigQuery will require additional permissions on a shared cluster if the user is not a workspace admin. This is due to the drivers not guaranteeing user isolation, e.g., as the driver writes data from multiple users to a widely accessible temp directory.

Workaround:
Granting ANY FILE permissions will allow users to access untrusted databases. Note that ANY FILE will still enforce ACLs on any tables or external (storage) locations governed by Unity Catalog.
This requires DBR 12.2 or later (DBR 12.1 or before is blocked on the network layer)



### AF305.1 - boto3

The `boto3` library is used.

Instance profiles (AWS) are not supported from the Python/Scala REPL or UDFs, e.g. using boto3 or s3fs, Instance profiles are only set from init scripts and (internally) from Spark. To access S3 objects the recommendation is to use EXTERNAL VOLUMES mapped to the fixed s3 storage location.

**Workarounds**
For accessing cloud storage (S3), use storage credentials and external locations.

(AWS) Consider other ways to authenticate with boto3, e.g., by passing credentials from Databricks secrets directly to boto3 as a parameter, or loading them as environment variables. This page contains more information. Please note that unlike instance profiles, those methods do not provide short-lived credentials out of the box, and customers are responsible for rotating secrets according to their security needs.



### AF305.2 - s3fs

The `s3fs` library is used which provides posix type sematics for S3 access. s3fs is based on boto3 library and has similar restrictions. The recommendation is to use EXTERNAL VOLUMES mapped to the fixed s3 storage location or MANAGED VOLUMES.



### AF306.1 - dbutils...getContext (`.toJson()`)

The `dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()` pattern was found. This function may trigger a security exception in DBR 13.0 and above.

The recommendation is to explore alternative APIs:
```
from dbruntime.databricks_repl_context import get_context
context = get_context()
context.__dict__
```



### AF306.2 - dbutils...getContext

The `dbutils.notebook.entry_point.getDbutils().notebook().getContext()` pattern was found. This function may trigger a security exception in DBR 13.0 and above.

```
from dbruntime.databricks_repl_context import get_context
context = get_context()
context.__dict__
```



### AF310.1 - credential passthrough (`dbutils.credentials.`)

The `dbutils.credentials.` is used for credential passthrough. This is not supported by Unity Catalog.



## AF311.x - dbutils (`dbutils`)

DBUtils and other clients that directly read the data from cloud storage are not supported.
Use [Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html) or use Assigned clusters.

### AF311.1 - dbutils.fs (`dbutils.fs.`)

The `dbutils.fs.` pattern was found. DBUtils and other clients that directly read the data from cloud storage are not supported. Please note that `dbutils.fs` calls with /Volumes ([Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)) will work.



### AF311.2 - dbutils mount(s) (`dbutils.fs.mount`)

The `dbutils.fs.mount` pattern was found. This is not supported by Unity Catalog. Use instead EXTERNAL LOCATIONS and VOLUMES.



### AF311.3 - dbutils mount(s) (`dbutils.fs.refreshMounts`)

The `dbutils.fs.refreshMounts` pattern was found. This is not supported by Unity Catalog. Use instead EXTERNAL LOCATIONS and VOLUMES.



### AF311.4 - dbutils mount(s) (`dbutils.fs.unmount`)

The `dbutils.fs.unmount` pattern was found. This is not supported by Unity Catalog. Use instead EXTERNAL LOCATIONS and VOLUMES.



### AF311.5 - mount points (`dbfs:/mnt`)

The `dbfs:/mnt` is used as a mount point. This is not supported by Unity Catalog. Use instead EXTERNAL LOCATIONS and VOLUMES.



### AF311.6 - dbfs usage (`dbfs:/`)

The `dbfs:/` pattern was found. DBFS is not supported by Unity Catalog. Use instead EXTERNAL LOCATIONS and VOLUMES. There may be false positives with this pattern because `dbfs:/Volumes/mycatalog/myschema/myvolume` is legitimate usage.

Please Note: `dbfs:/Volumes/<catalog>/<schema>/<volume>` is a supported access pattern for spark.



### AF311.7 - dbfs usage (`/dbfs/`)

The `/dbfs/` pattern was found. DBFS is not supported by Unity Catalog. Use instead EXTERNAL LOCATIONS and VOLUMES.



## AF313.x - SparkContext

Spark Context(sc), spark.sparkContext, and sqlContext are not supported for Scala in any Databricks Runtime and are not supported for Python in Databricks Runtime 14.0 and above with Shared Compute access mode due to security restrictions. In Shared Compute mode, these methods do not support strict data isolation.

To run legacy workloads without modification, use [access mode "Single User"](https://docs.databricks.com/en/compute/configure.html#access-modes) type clusters.

Databricks recommends using the spark variable to interact with the SparkSession instance.

The following sc functions are also not supported in "Shared" Access Mode: emptyRDD, range, init_batched_serializer, parallelize, pickleFile, textFile, wholeTextFiles, binaryFiles, binaryRecords, sequenceFile, newAPIHadoopFile, newAPIHadoopRDD, hadoopFile, hadoopRDD, union, runJob, setSystemProperty, uiWebUrl, stop, setJobGroup, setLocalProperty, getConf.

### AF313.1 - SparkContext (`spark.sparkContext`)

The `spark.sparkContext` pattern was found, use the `spark` variable directly.



### AF313.2 - SparkContext (`from pyspark.sql import SQLContext`)

The `from pyspark.sql import SQLContext` and `import org.apache.spark.sql.SQLContext` are used. These are not supported in Unity Catalog. Possibly, the `spark` variable will suffice.



### AF313.3 - SparkContext (`.binaryFiles`)

The `.binaryFiles` pattern was found, this is not supported by Unity Catalog.
Instead, please consider using `spark.read.format('binaryFiles')`.



### AF313.4 - SparkContext (`.binaryRecords`)

The `.binaryRecords` pattern was found, which is not supported by Unity Catalog Shared Compute access mode.



### AF313.5 - SparkContext (`.emptyRDD`)

The `.emptyRDD` pattern was found, which is not supported by Unity Catalog Shared Compute access mode.
Instead use:
```python
%python
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([StructField("k", StringType(), True)])
spark.createDataFrame([], schema)
```



### AF313.6 - SparkContext (`.getConf`)

The `.getConf` pattern was found. There may be significant false positives with this one as `.getConf` is a common API pattern. In the case of `sparkContext.getConf` or `sc.getConf`, use `spark.conf` instead.

- spark.conf.get() # retrieves a single value
- spark.conf.set() # sets a single value (If allowed)
- spark.conf.getAll() unfortunately does not exist



### AF313.7 - SparkContext ( `.hadoopFile` )

The `.hadoopFile` pattern was found. Use "Assigned" access mode compute or make permanent file type changes.



### AF313.8 - SparkContext ( `.hadoopRDD` )

The `.hadoopRDD` pattern was found. Use "Assigned" access mode compute or make permanent file type changes.



### AF313.9 - SparkContext ( `.init_batched_serializer` )

The `.init_batched_serializer` pattern was found. No suggestions available at this time.



### AF313.10 - SparkContext ( `.newAPIHadoopFile` )

The `.newAPIHadoopFile` pattern was found. Use "Assigned" access mode compute or make permanent file type changes.



### AF313.11 - SparkContext ( `.newAPIHadoopRDD` )

The `.newAPIHadoopRDD` pattern was found. Use "Assigned" access mode compute or make permanent file type changes.



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



### AF313.13 - SparkContext ( `.pickleFile` )

The `.pickleFile` pattern was found. Use "Assigned" access mode compute or make permanent file type changes.



### AF313.14 - SparkContext ( `.range` )

The `.range` pattern was found. Use `spark.range()` instead of `sc.range()`



### AF313.15 - SparkContext ( `.rdd` )

The `.rdd` pattern was found. Use "Assigned" access mode compute or upgrade to the faster Spark DataFrame api.



### AF313.16 - SparkContext ( `.runJob` )

The `.runJob` pattern was found. Use "Assigned" access mode compute or upgrade to the faster Spark DataFrame api.



### AF313.17 - SparkContext ( `.sequenceFile` )

The `.sequenceFile` pattern was found. Use "Assigned" access mode compute or make permanent file type changes.



### AF313.18 - SparkContext ( `.setJobGroup` )

The `.setJobGroup` pattern was found.
`spark.addTag()` can attach a tag, and `getTags()` and `interruptTag(tag)` can be used to act upon the presence/absence of a tag. These APIs only work with Spark Connect (Shared Compute Mode) and will not work in "Assigned" access mode.



### AF313.19 - SparkContext ( `.setLocalProperty` )

The `.setLocalProperty` pattern was found.



### AF313.20 - SparkContext ( `.setSystemProperty` )

The `.setSystemProperty` pattern was found. Use "Assigned" access mode compute or find alternative within the spark API.



### AF313.21 - SparkContext ( `.stop` )

The `.stop` pattern was found. Use "Assigned" access mode compute or find alternative within the spark API.



### AF313.22 - SparkContext ( `.textFile` )

The `.textFile` pattern was found. Use "Assigned" access mode compute or find alternative within the spark API.



### AF313.23 - SparkContext ( `.uiWebUrl`)

The `.uiWebUrl` pattern was found. Use "Assigned" access mode compute or find alternative within the spark API.



### AF313.24 - SparkContext (`.union`)

The `.union` pattern was found, Use "Assigned" access mode compute or find alternative within the spark API.



### AF313.25 - SparkContext (`.wholeTextFiles`)

The `.wholeTextFiles` pattern was found. Use "Assigned" access mode compute or use the `spark.read().text("file_name")` API.



## AF314.x - Distributed ML
Databricks Runtime ML and Spark Machine Learning Library (MLlib) are not supported on shared Unity Catalog compute. The recommendation is to use Assigned mode cluster; Use cluster policies and (warm) compute pools to improve compute and cost management.

### AF314.1 - Distributed ML (`sparknlp`)

The `sparknlp` pattern was found. Use "Assigned" access mode compute.



### AF314.2 - Distributed ML (`xgboost.spark`)

The `xgboost.spark` pattern was found. Use "Assigned" access mode compute.



### AF314.3 - Distributed ML (`catboost_spark`)

The `catboost_spark` pattern was found. Use "Assigned" access mode compute.



### AF314.4 - Distributed ML (`ai.catboost:catboost-spark`)

The `ai.catboost:catboost-spark` pattern was found. Use "Assigned" access mode compute.



### AF314.5 - Distributed ML (`hyperopt`)

The `hyperopt` pattern was found. Use "Assigned" access mode compute.



### AF314.6 - Distributed ML (`SparkTrials`)

The `SparkTrials` pattern was found. Use "Assigned" access mode compute.



### AF314.7 - Distributed ML (`horovod.spark`)

The `horovod.spark` pattern was found. Use "Assigned" access mode compute.



### AF314.8 - Distributed ML (`ray.util.spark`)

The `ray.util.spark` pattern was found. Use "Assigned" access mode compute.



### AF314.9 - Distributed ML (`databricks.automl`)

The `databricks.automl` pattern was found. Use "Assigned" access mode compute.



### AF308.1 - Graphframes (`from graphframes`)

The `from graphframes` pattern was found. Use "Assigned" access mode compute.



### AF309.1 - Spark ML (`pyspark.ml.`)

The `pyspark.ml.` pattern was found. Use "Assigned" access mode compute.



### AF315.1 - UDAF scala issue (`UserDefinedAggregateFunction`)

The `UserDefinedAggregateFunction` pattern was found. Use "Assigned" access mode compute.



### AF315.2 - applyInPandas (`applyInPandas`)

The `applyInPandas` pattern was found. Use "Assigned" access mode compute.



### AF315.3 - mapInPandas (`mapInPandas`)

The `mapInPandas` pattern was found. Use "Assigned" access mode compute.




## AF330.x - Streaming
Streaming limitations for Unity Catalog shared access mode [documentation](https://docs.databricks.com/en/compute/access-mode-limitations.html#streaming-limitations-for-unity-catalog-shared-access-mode) should be consulted for more details.

See also [Streaming limitations for Unity Catalog single user access mode](https://docs.databricks.com/en/compute/access-mode-limitations.html#streaming-single) and [Streaming limitations for Unity Catalog shared access mode](https://docs.databricks.com/en/compute/access-mode-limitations.html#streaming-shared).

The assessment patterns and specifics are as follows:

### AF330.1 - Streaming (`.trigger(continuous`)

The `.trigger(continuous` pattern was found. Continuous processing mode is not supported in Unity Catalog shared access mode.
Apache Spark continuous processing mode is not supported. See [Continuous Processing](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#continuous-processing) in the Spark Structured Streaming Programming Guide.



### AF330.2 - Streaming (`kafka.sasl.client.callback.handler.class`)

The `kafka.sasl.client.callback.handler.class` pattern was found. SASL features are not supported in Unity Catalog shared access mode.



### AF330.3 - Streaming (`kafka.sasl.login.callback.handler.class`)

The `kafka.sasl.login.callback.handler.class` pattern was found. SASL features are not supported in Unity Catalog shared access mode.



### AF330.4 - Streaming (`kafka.sasl.login.class`)

The `kafka.sasl.login.class` pattern was found. SASL features are not supported in Unity Catalog shared access mode.



### AF330.5 - Streaming (`kafka.partition.assignment.strategy`)

The `kafka.partition.assignment.strategy` pattern was found. Kafka features are not supported in Unity Catalog shared access mode.



### AF330.6 - Streaming (`kafka.ssl.truststore.location`)

The `kafka.ssl.truststore.location` pattern was found. SSL features are not supported in Unity Catalog shared access mode.



### AF330.7 - Streaming (`kafka.ssl.keystore.location`)

The `kafka.ssl.keystore.location` pattern was found. SSL features are not supported in Unity Catalog shared access mode.



### AF330.8 - Streaming (`cleanSource`)

The `cleanSource` pattern was found. The cleanSource operation is not supported in Unity Catalog shared access mode.



### AF330.9 - Streaming (`sourceArchiveDir`)

The `sourceArchiveDir` pattern was found. The sourceArchiveDir operation is not supported in Unity Catalog shared access mode.



### AF330.10 - Streaming (`applyInPandasWithState`)

The `applyInPandasWithState` pattern was found. The applyInPandasWithState operation is not supported in Unity Catalog shared access mode.



### AF330.11 - Streaming (`.format("socket")`)

The `.format("socket")` pattern was found. Socket source is not supported in Unity Catalog shared access mode.



### AF330.12 - Streaming (`StreamingQueryListener`)

The `StreamingQueryListener` pattern was found. StreamingQueryListener is not supported in Unity Catalog shared access mode.



### AF330.13 - Streaming (`applyInPandasWithState`)

The `applyInPandasWithState` pattern was found. The applyInPandasWithState operation is not supported in Unity Catalog shared access mode.




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

[EXTERNAL LOCATION](https://docs.databricks.com/en/connect/unity-catalog/external-locations.html#create-an-external-location) is a UC object type describing a url to a cloud storage bucket + folder or storage account + container and folder

## STORAGE CREDENTIAL

[STORAGE CREDENTIAL](https://docs.databricks.com/en/sql/language-manual/sql-ref-storage-credentials.html) is a UC object encapsulating the credentials necessary to access cloud storage.

## Assigned Clusters or Single User Clusters
"Assigned Clusters" are Interactive clusters assigned to a single principal. Implicit in this term is that these clusters are enabled for Unity Catalog. Publicly available today, "Assigned Clusters" can be assigned to a user and the user's identity is used to access data resources. The access to the cluster is restricted to that single user to ensure accountability and accuracy of the audit logs.

"Single User Clusters" are Interactive clusters that name one specific user account as user.

The `data_security_mode` for these clusters are `SINGLE_USER`

## Shared Clusters
"Shared Clusters are Interactive or Job clusters with an access mode of "SHARED".


The `data_security_mode` for these clusters are `USER_ISOLATION`.



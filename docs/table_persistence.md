# UCX objects

List of all UCX objects and their respective metadata.

## Overview

Table Utilization:

| Table                    | Generate Assessment | Migrate Local Groups | Migrate External Tables | Migrate SQL Warehouses | Upgrade Jobs | Migrate managed tables |
|--------------------------|---------------------|----------------------|-------------------------|------------------------|--------------|------------------------|
| tables                   | RW                  |                      | RO                      |                        |              | RO                     |
| grants                   |                     |                      | RW                      |                        |              | RW                     |
| mounts                   | RW                  |                      | RO                      |                        | RO           | RO                     |
| permissions              |                     | RW                   |                         |                        |              |                        |
| jobs                     | RW                  |                      |                         |                        | RO           |                        |
| clusters                 | RW                  |
| external_locations       | RW                  |                      | RO                      |
| workspace                | RW                  | RO                   |                         |                        | RO           |
| workspace_objects        |                     |                      |                         |                        |              |
| azure_service_principals |                     |                      |                         |                        |              |
| global_init_scripts      |                     |                      |                         |                        |              |
| pipelines                |                     |                      |                         |                        |              |
| groups                   |                     |                      |                         |                        |              |
| table_size               |                     |                      |                         |                        |              |
| table_failures           |                     |                      |                         |                        |              |
| submit_runs              |                     |                      |                         |                        |              |
| policies                 |                     |                      |                         |                        |              |
| migration_status         |                     |                      |                         |                        |              |
| workflow_problems        |                     |                      |                         |                        |              |
| udfs                     |                     |                      |                         |                        |              |
| logs                     |                     |                      |                         |                        |              |
| recon_results            |                     |                      |                         |                        |              |

**RW** - Read/Write the job that generates the table
**RO** - Read Only

### Inventory Database

#### _$inventory_.tables

Holds Inventory of all tables in all databases and their relevant metadata.

| Column       | Datatype        | Description                                                                 | Comments |
|--------------|-----------------|-----------------------------------------------------------------------------|----------|
| catalog      | string          | Original catalog of the table. _hive_metastore_ by default                  |
| database     | string          | Original schema of the table                                                |
| name         | string          | Name of the table                                                           |
| object_type  | string          | MANAGED, EXTERNAL, or VIEW                                                  |
| table_format | string          | Table provider. Like delta or json or parquet.                              |
| location     | string          | Location of the data for table                                              |
| view_text    | nullable string | If the table is the view, then this column holds the definition of the view |
| upgraded_to  | string          | Upgrade Target (3 level namespace)                                          |

<br/>

#### _$inventory_.table_failures

Holds failures that occurred during crawling HMS tables

| Column   | Datatype | Description                                              | Comments |
|----------|----------|----------------------------------------------------------|----------|
| catalog  | string   | Original catalog of the table. hive_metastore by default |
| database | string   | Original schema of the table                             |
| name     | string   | Name of the table                                        |
| failures | string   | Exception message context                                |

<br/>

#### _$inventory_.grants

Inventory of all Table ACLs for tables indexed in tables table.

| Column             | Datatype        | Description                                              | Comments |
|--------------------|-----------------|----------------------------------------------------------|----------|
| principal          | string          | User name, group name, or service principal name         |
| action_type        | string          | Name of GRANT action                                     |
| catalog            | string          | Original catalog of the table. hive_metastore by default |
| database           | Nullable string | Original schema of the table                             |
| table              | Nullable string | Name of the table                                        |
| view               | Nullable string | Name of the view                                         |
| any_file           | bool            | Any file                                                 |
| anonymous_function | string          | Grant for the anonymous function                         |

<br/>

#### _$inventory_.mounts

List of DBFS mount points.

| Column           | Datatype        | Description                                                            | Comments |
|------------------|-----------------|------------------------------------------------------------------------|----------|
| name             | string          | Name of the mount point                                                |
| source           | string          | Location of the backing dataset                                        |
| instance_profile | Nullable string | This mount point is accessible only with this AWS IAM instance profile |

<br/>

#### _$inventory_.permissions

Workspace object level permissions

| Column                 | Datatype | Description                                                                                                                                                                                     | Comments |
|------------------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| object_id              | string   | Either:<br/>Group ID<br/>Workspace Object ID<br/>Redash Object ID<br/>Scope name                                                                                                                                                         |          |
| supports               | string   | One of:<br/>AUTHORIZATION<br/><br/>CLUSTERS<br/>CLUSTER_POLICIES<br/>DIRECTORIES<br/>EXPERIMENTS<br/>FILES<br/>INSTANCE_POOLS<br/>JOBS<br/>NOTEBOOKS<br/>PIPELINES<br/>REGISTERED_MODELS<br/>REPOS<br/>SERVING_ENDPOINTS<br/>SQL_WAREHOUSES |          |
| raw_object_permissions | JSON     | JSON-serialized response of:<br/>Generic Permissions<br/>Secret ACL<br/>Group roles and entitlements<br/>Redash permissions                                                                                                              |          |

<br/>

#### _$inventory_.jobs

Holds a list of all jobs with a notation of potential issues.

| Column      | Datatype | Description                                                | Comments |
|-------------|----------|------------------------------------------------------------|----------|
| job_id      | string   | Job ID                                                     |
| job_name    | string   | Job Name                                                   |
| job_creator | string   | UserID of the Job Creator                                  |
| compatible  | int      | 1 or 0, used for percentage reporting                      |
| failures    | string   | List of issues identified by the assessment in JSON format |

#### _$inventory_.clusters

Holds a list of all clusters with a notation of potential issues.

| Column          | Datatype | Description                                                | Comments |
|-----------------|----------|------------------------------------------------------------|----------|
| cluster_id      | string   | Cluster Id                                                 |
| cluster_name    | string   | Cluster Name                                               |
| cluster_creator | string   | UserID of the Cluster Creator                              |
| compatible      | int      | 1 or 0, used for percentage reporting                      |
| failures        | string   | List of issues identified by the assessment in JSON format |

#### _$inventory_.external_locations

Holds a list of all external locations that will be required for the migration.

| Column            | Datatype | Description           | Comments |
|-------------------|----------|-----------------------|----------|
| external_location | string   | External Location URL |

#### _$inventory_.workspace_objects

Holds a list of all workspace objects (notebooks, directories, files, repos and libraries) from the workspace.
This is used by the permission crawler.

| Column      | Datatype | Description                                               | Comments |
|-------------|----------|-----------------------------------------------------------|----------|
| object_id   | string   | unique id of the object                                   |
| object_type | string   | type of object (NOTEBOOK, DIRECTORY, REPO, FILE, LIBRARY) |
| path        | string   | full path of the object in the workspace                  |
| language    | string   | language of the object (applicable for notebooks only)    |


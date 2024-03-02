# Table Upgrade logic and data structures

The Hive Metastore migration process will upgrade the following Assets:

- Tables ond DBFS root
- External Tables
- Views

We don't expect this process to be a "one and done" process. This typically is an iterative process and may require a
few runs.

We suggest to keep track of the migration and provide the user a continuous feedback of the progress and status of the
upgrade.

The migration process will be set as a job that can be invoked multiple times.
Each time it will upgrade tables it can and report the ones it can't.

## Common considerations

1. One view per workspace summarizing all the table inventory and various counters
1. By default we create a single catalog per HMS (<prefix (optional)>_<workspace_name>), happens at the account level.
1. Workspace Name would be set up as part of the installation at the account level.
1. Consider other mappings of environments/database to catalog/database.
    1. The user will be able to specify a default catalog for the workspace.
1. We have to annotate the status of assets that were migrated.
1. We will roll up the migration status to the workspace/account level. Showing migration state.
1. Aggregation of migration failures
    1. View of object migration:

   | Object Type | Object ID | Migrated | Migration Failures |
      |----|----|----|----|
   |View|hive_metastore.finance.transaction_vw|1|[]|
   |Table|hive_metastore.finance.transactions|0|["Table uses SERDE: csv"]|
   |Table|hive_metastore.finance.accounts|0|[]|
   |Cluster|klasd-kladef-01265|0|["Uses Passthru authentication"]|

1. By default the target is the target_catalog/database_name
1. The assessment will generate a mapping file/table. The file will be in CSV format.

   | Source Database | Target Catalog | Target Database |
      |----|----|----|
   |finance| de_dev | finance |
   |hr | de_dev | human_resources|
   |sales | ucx-dev_ws | sales |
1. The user can download the mapping file, override the targets and upload it to the workspace .csx folder.
1. By default we copy the table content (CTAS)
1. Allow skipping individual tables/databases
1. Explore sizing tables or another threshold (recursively count bytes)
1. By default we copy the table into a managed table/managed location
1. Allow overriding target to an external table
1. We should migrate ACLs for the tables (where applicable). We should highlight cases where we can't (no direct
   translation/conflicts)
1. We should consider automating ACLs based on Instance Profiles / Service Principals and other legacy security
   mechanisms

## Tables (Parquet/Delta) on DBFS root

1. By default we copy the table content (CTAS)
1. Allow skipping individual tables/databases
1. Explore sizing tables or another threshold (recursively count bytes)
1. By default we copy the table into a managed table/managed location
1. Allow overriding target to an external table
1. Allow an exception list in case we want to skip certain tables

## Tables (Parquet/Delta) on Cloud Storage

1. Verify that we have the external locations for these tables
1. Automate creation of External Locations (Future)
1. Use sync to upgrade these tables "in place". Use the default or override catalog.database destination.
1. Update the source table with "upgraded_to" property

## Tables (None Parquet/Delta)

1. Copy this table using CTAS or Deep Clone. (Consider bringing history)
1. Copy the Metadata
1. Skip tables as needed based on size threshold or an exception list
1. Update the source table with "upgraded_to" property

## Views

1. Make a "best effort" attempt to upgrade view
1. Create a view in the new location
1. Upgrade table reference to the new tables (based on the upgraded_to table property)
1. Handle nested views
1. Handle or highlight other cases (functions/storage references/ETC)
1. Create an exception list with views failures

## Functions

1. We should migrate (if possible) functions
1. Address incompatibilities

## Account Consideration

1. HMS on multiple workspaces may point to the same assets. We need to dedupe upgrades.
1. Allow running assessment on all the accounts workspaces or on a group of workspaces.
1. We have to test on Glue and other external Metastores
1. Create an exception list at the account level the list should contain
    1. Tables that show up on more than one workspace (pointing to the same cloud storage location)
    1. Tables that show up on more than one workspace with different metadata
    1. Tables that show up on more than one workspace with different ACLs
1. Addressing table conflicts/duplications require special processing we have the following options
    1. Define a "master" and create derivative objects as views
    1. Flag and skip the dupes
    1. Duplicate the data and create dupes
1. Consider upgrading a workspace at a time. Highlight the conflict with prior upgrades.
1. Allow workspace admins to upgrade more than one workspace.

## Open Questions

1. How do we manage/surface potential cost of the assessment run in case of many workspaces.
1. How do we handle conflicts between workspaces
1. What mechanism do we use to map source to target databases
1. How to list workspaces in Azure/AWS

----


The code provided is a Python module that defines a `Grant` dataclass and a `GrantsCrawler` class. The `Grant` dataclass represents a grant of privileges in a database system, with attributes for the principal, action type, catalog, database, table, view, UDF, and flags for any file and anonymous function. The `GrantsCrawler` class is a crawler that fetches grants for databases, tables, views, UDFs, and anonymous functions in a Hive metastore. It uses a `TablesCrawler` and `UdfsCrawler` to fetch table and UDF information, respectively. The `GrantsCrawler` class provides methods for fetching grants based on different parameters and returning them as an iterable of `Grant` objects. It also provides methods for getting grants for a specific table or schema. The code includes a `_type_and_key` method that normalizes the input parameters and returns a tuple of the object type and key, which is used to fetch grants for the specified object. The code also includes methods for generating SQL statements to grant and revoke privileges in Hive and Unity Catalog (UC) systems.


The code provided is a part of a software project, specifically a Python module for working with Databricks' labs Unified Data Analytics Platform (UCAP). The module includes two classes, `ExternalLocations` and `Mounts`, which inherit from `CrawlerBase`.

`ExternalLocations` is a class for crawling and managing external locations used by tables in a Databricks workspace. It has methods for creating a list of external locations based on tables in a given schema and a method for generating Terraform definitions for any missing external locations. The class has a `_external_locations` method that filters and processes the external locations based on certain conditions.

`Mounts` is a class for managing mounts in a Databricks workspace. It has methods for listing and deduplicating mounts, as well as a method for creating a snapshot of the current mounts. The `_deduplicate_mounts` method removes any duplicate mounts based on their name and source.

Both classes use a common base class `CrawlerBase` which provides common functionality for crawling and managing resources in a Databricks workspace. The `CrawlerBase` class has a `_snapshot` method that returns an iterable of resources based on a try fetch function and a list function. The `CrawlerBase` class also has a `_try_fetch` method that fetches resources from a Databricks workspace and a `_fetch` method that executes a SQL query and returns the result.


The code provided is a Python module for managing table mappings in a data migration process. It defines two dataclasses, `Rule` and `TableToMigrate`, which encapsulate information about the source and target tables for migration. The `TableMapping` class is the main class for managing table mappings, and it includes methods for loading and saving the mappings to a file, skipping tables and schemas, and checking if a table is already migrated or marked to be skipped. The class also includes methods for initializing and interacting with a `WorkspaceClient` instance, which is used to execute SQL queries against a databricks workspace. The module also defines various utility functions for working with tables and rules. The goal of this module is to provide a flexible and extensible way to manage table mappings during a data migration process.

1. The `Rule` dataclass represents a single rule in the table mapping. It contains information about the source and target catalog, schema, and table names. It also includes a method for generating the unique key for the target table in the Unity Catalog (UC) and the Hive Metastore (HMS). The `initial` class method creates a new `Rule` instance with default values.
2. The `TableToMigrate` dataclass represents a single table that needs to be migrated. It contains a `Table` object representing the source table and a `Rule` object representing the migration rule for that table.
3. The `TableMapping` class is the main class for managing table mappings. It is initialized with an `Installation` object, a `WorkspaceClient` object, and a `SqlBackend` object. These objects are used to interact with the Unity Catalog, the workspace, and to execute SQL queries. The class includes several methods for managing the table mappings:
* `current`: A class method that returns a new `TableMapping` instance with the current installation, workspace client, and SQL backend.
* `current_tables`: A method that returns a generator that yields `Rule` instances for all the current tables in the workspace. The method takes a `TablesCrawler` object and the names of the workspace and catalog.
* `save`: A method that saves the current tables to a file in the installation. The method takes a `TablesCrawler` object and a `WorkspaceInfo` object and returns the filename of the saved file.
* `load`: A method that loads the table mappings from a file in the installation. The method returns a list of `Rule` instances.
* `skip_table`: A method that marks a table to be skipped in the migration process by applying a table property. The method takes the schema and table names as arguments.
* `skip_schema`: A method that marks a schema to be skipped in the migration process by applying a schema property. The method takes the schema name as an argument.
* `get_tables_to_migrate`: A method that returns a list of `TableToMigrate` instances for all the tables that need to be migrated. The method takes a `TablesCrawler` object and returns a list of `TableToMigrate` instances.
* `_get_databases_in_scope`: A method that returns a list of databases that are not marked to be skipped. The method takes a set of database names and returns a list of database names.
* `_get_database_in_scope_task`: A method that checks if a database is marked to be skipped. The method takes a database name and returns None if the database is marked to be skipped, or the database name otherwise.
* `_get_table_in_scope_task`: A method that checks if a table is marked to be skipped and if it already exists in the Unity Catalog. The method takes a `TableToMigrate` instance and returns None if the table is marked to be skipped or if it already exists in the Unity Catalog, or the `TableToMigrate` instance otherwise.
* `exists_in_uc`: A method that checks if a table already exists in the Unity Catalog. The method takes a `Table` object and the target key and returns True if the table exists, False otherwise.
4. The module also includes several utility functions for working with tables and rules:
* `escape_sql_identifier`: A function that escapes a SQL identifier.
* `TablesCrawler.snapshot`: A method that returns a list of `Table` instances for all the tables in the workspace.
* `TablesCrawler.parse_database_props`: A static method that parses the properties of a database.
* `Table.sql_unset_upgraded_to`: A method that generates the SQL query for unsetting the `upgraded_to` property of a table.

In summary, the provided module is a comprehensive solution for managing table mappings during a data migration process. It includes classes and methods for defining and manipulating table mappings, as well as utility functions for working with tables and rules. The `TableMapping` class is the main class for managing table mappings, and it includes methods for loading and saving the mappings to a file, skipping tables and schemas, and checking if a table is already migrated or marked to be skipped. The `Rule` and `TableToMigrate` dataclasses encapsulate information about the source and target tables for migration. The utility functions provide a convenient way to work with tables and rules.


The code provided is a Python module that contains two classes, `TablesMigrate` and `TableMove`. 

The `TablesMigrate` class is responsible for migrating tables from one schema to another in a Databricks workspace. It takes an instance of `TablesCrawler`, `WorkspaceClient`, `SqlBackend`, and `TableMapping` as input. The class has several methods such as `migrate_tables`, `_migrate_table`, `_migrate_external_table`, `_migrate_dbfs_root_table`, `_migrate_view`, `_init_seen_tables`, `_table_already_upgraded`, `_get_tables_to_revert`, `_revert_migrated_table`, `_get_revert_count`, `is_upgraded`, and `print_revert_report`. The `migrate_tables` method takes an optional argument `what` which is used to filter the tables based on their type. The `_migrate_table` method is responsible for migrating the actual table and calls the appropriate method based on the type of the table. The `_revert_migrated_table` method is used to revert the migration of a table. The `_get_revert_count` method is used to get the count of tables that can be reverted. The `is_upgraded` method checks if a table has been upgraded or not. The `print_revert_report` method prints the report of the tables that can be reverted.

The `TableMove` class is responsible for moving or aliasing tables and views from one schema to another in a Databricks workspace. It takes an instance of `WorkspaceClient` and `SqlBackend` as input. The class has several methods such as `move_tables`, `alias_tables`, `_move_table`, `_alias_table`, `_move_view`, `_reapply_grants`, `_recreate_table`, `_create_alias_view`, and `_recreate_view`. The `move_tables` method is used to move tables from one schema to another. The `alias_tables` method is used to create aliases of tables and views in another schema. The `_move_table` and `_alias_table` methods are responsible for moving and aliasing the actual table or view. The `_move_view` method is used to move a view from one schema to another. The `_reapply_grants` method is used to reapply the grants on the migrated table or view. The `_recreate_table` and `_recreate_view` methods are used to recreate the table or view in the destination schema.


The code is a part of a data crawler system that calculates the size of tables in a Hive Metastore. It defines a class `TableSizeCrawler` that inherits from `CrawlerBase`. This class is initialized with a SQL Execution Backend and a schema name. It uses the `TablesCrawler` class to obtain a snapshot of tables, and then iterates over them to calculate the size of each table. The `_safe_get_table_size` method calculates the size of a table by querying the Spark SQL engine. If the table does not exist, the method returns `None`. The `snapshot` method returns a list of `TableSize` objects representing the snapshot of tables, filtered to include only those with a non-null size. The `_try_load` method tries to load table information from the database, and raises a `TABLE_OR_VIEW_NOT_FOUND` error if the table cannot be found. The `_crawl` method crawls and lists tables using the `tables_crawler` object, and calculates the size of DBFS root tables. The method skips tables that are not of type `TABLE` or are not DBFS root tables. The `TableSize` dataclass is used to store the catalog, database, name, and size in bytes of each table. The `logger` object is used to log debug, warning, and error messages. The `functools.partial` function is used to create partial functions for the `_try_load` and `_crawl` methods. The `collections.abc.Iterable` abstract base class is used to ensure that the `_crawl` and `_try_load` methods return iterable objects. The `dataclasses.dataclass` decorator is used to define the `TableSize` dataclass. The `databricks.labs.ucx.framework.crawlers.CrawlerBase` and `databricks.labs.ucx.framework.crawlers.SqlBackend` classes are imported from the `databricks` package, as well as the `databricks.labs.ucx.hive_metastore.TablesCrawler` class. The `logging` module is imported to log messages. The `pyspark.sql.session.SparkSession` class is imported to create a SparkSession object to interact with the Spark SQL engine.


The provided code is a Python script that defines a `Table` class, which represents a table or view in a database. The `Table` class has various properties and methods to extract information about the table such as its kind (table or view), catalog, database, name, table format, location, and view text. The class also has methods to generate SQL commands for altering, migrating, and unsetting the upgraded\_to property of the table.

The code also defines a `TablesCrawler` class, which is a subclass of `CrawlerBase` and is used to crawl and list tables in a specified catalog and database. The `TablesCrawler` class has methods to take a snapshot of tables, fetch table information, and describe table metadata. The `TablesCrawler` class uses a SQL Execution Backend abstraction to execute SQL queries and fetch results.

The `TableError` and `MigrationCount` classes are also defined in the code, but they are not used in the provided code.

The `Table` class has the following methods:

* `is_delta`: Returns `True` if the table format is DELTA, else `False`.
* `key`: Returns the key of the table in the format `<catalog>.<database>.<name>`.
* `kind`: Returns the kind of the table, either 'TABLE' or 'VIEW'.
* `sql_alter_to`: Returns an SQL command to alter the table to a target table key.
* `sql_alter_from`: Returns an SQL command to alter the table from a target table key with a workspace ID.
* `sql_unset_upgraded_to`: Returns an SQL command to unset the upgraded\_to property of the table.
* `is_dbfs_root`: Returns `True` if the table location is in the DBFS root directory, else `False`.
* `is_format_supported_for_sync`: Returns `True` if the table format is supported for synchronization, else `False`.
* `is_databricks_dataset`: Returns `True` if the table location is a Databricks dataset, else `False`.
* `what`: Returns the type of the table based on its properties.
* `sql_migrate_external`: Returns an SQL command to migrate an external table.
* `sql_migrate_dbfs`: Returns an SQL command to migrate a table located in DBFS.
* `sql_migrate_view`: Returns an SQL command to migrate a view.

The `TablesCrawler` class has the following methods:

* `_all_databases`: Returns an iterator of rows containing all databases.
* `snapshot`: Takes a snapshot of tables in the specified catalog and database.
* `_parse_table_props`: Parses table properties and returns them as a dictionary.
* `parse_database_props`: Parses database properties and returns them as a dictionary.
* `_try_load`: Tries to load table information from the database or throws a TABLE\_OR\_VIEW\_NOT\_FOUND error.
* `_crawl`: Crawls and lists tables within the specified catalog and database.
* `_safe_norm`: Returns a normalized string, either lowercased or not.
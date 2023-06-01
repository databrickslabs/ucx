# Databricks notebook source
# MAGIC %md
# MAGIC ##Backup catalog
# MAGIC
# MAGIC This notebook will read from the information_schema of a given catalog and dump its contents to external storage.
# MAGIC
# MAGIC This external storage will be independent from the UC storage and will be accessible by a remote workspace on a different region with a different UC.
# MAGIC
# MAGIC Assumptions:
# MAGIC - Dump and restore one catalog at a time (currently overwrites to the same folder)

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("storageLocation", "/mnt/externallocation", "Storage location for copy")
dbutils.widgets.text("catalogName", "system", "information_schema catalog")
dbutils.widgets.dropdown("getExternalLocations", "True", ["True", "False"])

# COMMAND ----------

storage_location = dbutils.widgets.get("storageLocation")
catalog_name = dbutils.widgets.get("catalogName")
get_external_location = dbutils.widgets.get("getExternalLocations")

table_list = spark.catalog.listTables(f"{catalog_name}.information_schema")

# COMMAND ----------

for table in table_list:
    info_schema_table_df = spark.sql(f"SELECT * FROM {table.catalog}.information_schema.{table.name}")
    info_schema_table_df.write.format("delta").mode("overwrite").save(f"{storage_location}/{table.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Optional step
# MAGIC Get table locations from running DESCRIBE EXTENDED on each table on information_schema.tables

# COMMAND ----------

if get_external_location:
    table_location_columns = [
        "table_catalog",
        "table_schema",
        "table_name",
        "table_location",
    ]
    table_location_storage = "external_table_locations"
    location_list = []

    # Need to filter out Unity Catalog data source that counts as external
    describe_table_list = spark.read.table(f"{catalog_name}.information_schema.tables").filter(
        "table_type=='EXTERNAL' AND data_source_format <> 'UNITY_CATALOG'"
    )

    for d_table in describe_table_list.collect():
        d_location = (
            spark.sql(f"DESCRIBE EXTENDED {d_table.table_catalog}.{d_table.table_schema}.{d_table.table_name}")
            .filter("col_name = 'Location'")
            .select("data_type")
            .head()[0]
        )
        location_list.append(
            [
                d_table.table_catalog,
                d_table.table_schema,
                d_table.table_name,
                d_location,
            ]
        )

    location_df = spark.createDataFrame(data=location_list, schema=table_location_columns)

    # merge with information_schema.tables and save external locations to storage_sub_directory column (that as of 03/09 only holds Managed table information)
    table_df = DeltaTable.forPath(spark, f"{storage_location}/tables")
    # table_df = spark.sql(f"SELECT * FROM {table.catalog}.information_schema.tables")
    table_df.alias("tables").merge(
        location_df.alias("locations"),
        "tables.table_catalog = locations.table_catalog and tables.table_schema = locations.table_schema and tables.table_name = locations.table_name",
    ).whenMatchedUpdate(set={"storage_sub_directory": "locations.table_location"}).execute()

    display(table_df)
    # or create a separate table only for this
    # (location_df
    # .write
    # .mode("overwrite")
    # .format("delta")
    # .save(f"{storage_location}/{table_location_storage}"))

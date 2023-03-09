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

dbutils.widgets.removeAll()
dbutils.widgets.text("storageLocation", "/mnt/externallocation", "Storage location for copy")
dbutils.widgets.text("catalogName", "system", "information_schema catalog")

# COMMAND ----------

storage_location = dbutils.widgets.get("storageLocation")
catalog_name =  dbutils.widgets.get("catalogName")

table_list = spark.catalog.listTables(f"{catalog_name}.information_schema")

# COMMAND ----------

for table in table_list:
    df = spark.sql(f"SELECT * FROM {table.catalog}.information_schema.{table.name}")
    df.write.format("delta").mode("overwrite").save(f"{storage_location}/{table.name}")

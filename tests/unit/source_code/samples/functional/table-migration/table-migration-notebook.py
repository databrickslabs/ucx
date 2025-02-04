# Databricks notebook source
# MAGIC %md
# MAGIC #Test notebook for Use tracking in Notebooks

# COMMAND ----------

# ucx[table-migrated-to-uc-python:+1:8:+1:29] Table people is migrated to cata4.nondefault.newpeople in Unity Catalog
display(spark.table('people')) # we are looking at default.people table

# COMMAND ----------

# MAGIC %sql USE something

# COMMAND ----------

# ucx[table-migrated-to-uc-python:+1:8:+1:30] Table persons is migrated to cata4.newsomething.persons in Unity Catalog
display(spark.table('persons')) # we are looking at something.persons table

# COMMAND ----------

spark.sql('USE whatever')

# COMMAND ----------

# ucx[table-migrated-to-uc-python:+1:8:+1:30] Table kittens is migrated to cata4.felines.toms in Unity Catalog
display(spark.table('kittens')) # we are looking at whatever.kittens table

# COMMAND ----------

# ucx[table-migrated-to-uc-python:+2:0:+2:38] Table numbers is migrated to cata4.counting.numbers in Unity Catalog
# ucx[default-format-changed-in-dbr8:+1:0:+1:38] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
spark.range(10).saveAsTable('numbers') # we are saving to whatever.numbers table.

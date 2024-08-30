# Databricks notebook source
# MAGIC %md # This is a Python notebook, that has SQL cell embedded

# COMMAND ----------

# ucx[direct-file-system-access:+1:8:+1:43] The use of direct file system access is deprecated: /mnt/things/e/f/g
display(spark.read.csv('/mnt/things/e/f/g'))

# COMMAND ----------

# ucx[direct-file-system-access-in-sql-query:+0:0:+0:1024] The use of direct file system access is deprecated: dbfs:/mnt/foo
# MAGIC %sql  SELECT * FROM csv.`dbfs:/mnt/foo`

# COMMAND ----------

# MAGIC %md mess around with formatting

# COMMAND ----------

# ucx[direct-file-system-access-in-sql-query:+0:0:+0:1024] The use of direct file system access is deprecated: dbfs:/mnt/bar/e/f/g
# MAGIC %sql
# MAGIC SELECT * FROM
# MAGIC   csv.`dbfs:/mnt/bar/e/f/g`
# MAGIC WHERE _c1 > 5

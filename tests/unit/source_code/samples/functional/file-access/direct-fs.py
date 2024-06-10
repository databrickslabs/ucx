# Databricks notebook source
# MAGIC %md # This is a Python notebook, that has SQL cell embedded

# COMMAND ----------

# ucx[dbfs-usage:+2:23:+2:40] Deprecated file system path in call to: /mnt/things/e/f/g
# ucx[direct-filesystem-access:+1:8:+1:43] The use of default dbfs: references is deprecated: /mnt/things/e/f/g
display(spark.read.csv('/mnt/things/e/f/g'))

# COMMAND ----------

# ucx[dbfs-query:+0:0:+0:1024] The use of DBFS is deprecated: dbfs:/mnt/foo
# MAGIC %sql  SELECT * FROM csv.`dbfs:/mnt/foo`

# COMMAND ----------

# MAGIC %md mess around with formatting

# COMMAND ----------

# ucx[dbfs-query:+0:0:+0:1024] The use of DBFS is deprecated: dbfs:/mnt/bar/e/f/g
# MAGIC %sql
# MAGIC SELECT * FROM
# MAGIC   csv.`dbfs:/mnt/bar/e/f/g`
# MAGIC WHERE _c1 > 5

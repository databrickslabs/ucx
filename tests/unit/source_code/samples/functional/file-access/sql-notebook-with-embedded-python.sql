-- Databricks notebook source
-- MAGIC %md # This is a SQL notebook, that has Python cell embedded

-- COMMAND ----------

-- ucx[dbfs-read-from-sql-query:+0:0:+0:1024] The use of DBFS is deprecated: dbfs:/mnt/whatever
SELECT * FROM csv.`dbfs:/mnt/whatever`





-- COMMAND ----------

-- MAGIC %python
-- ucx[implicit-dbfs-usage:+2:8:+2:43] The use of default dbfs: references is deprecated: /mnt/things/e/f/g
-- ucx[dbfs-usage:+1:23:+1:42] Deprecated file system path: /mnt/things/e/f/g
-- MAGIC display(spark.read.csv('/mnt/things/e/f/g'))


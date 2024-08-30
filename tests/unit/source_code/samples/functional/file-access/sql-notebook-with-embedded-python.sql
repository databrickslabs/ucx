-- Databricks notebook source
-- MAGIC %md # This is a SQL notebook, that has Python cell embedded

-- COMMAND ----------

-- ucx[direct-file-system-access-in-sql-query:+0:0:+0:1024] The use of direct file system access is deprecated: dbfs:/mnt/whatever
SELECT * FROM csv.`dbfs:/mnt/whatever`





-- COMMAND ----------

-- MAGIC %python
-- ucx[direct-file-system-access:+0:0:+0:1024] The use of direct file system access is deprecated: /mnt/things/e/f/g
-- MAGIC display(spark.read.csv('/mnt/things/e/f/g'))

-- Databricks notebook source
-- MAGIC %md # This is a SQL notebook, that has Python cell embedded

-- COMMAND ----------

-- ucx[direct-filesystem-access-in-sql-query:+0:0:+0:1024] The use of direct filesystem references is deprecated: dbfs:/mnt/whatever
SELECT * FROM csv.`dbfs:/mnt/whatever`





-- COMMAND ----------

-- MAGIC %python
-- ucx[direct-filesystem-access:+1:8:+1:43] The use of direct filesystem references is deprecated: /mnt/things/e/f/g
-- MAGIC display(spark.read.csv('/mnt/things/e/f/g'))


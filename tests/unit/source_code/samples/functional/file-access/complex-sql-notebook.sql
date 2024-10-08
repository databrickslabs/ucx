-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Test notebook for DBFS discovery in Notebooks

-- COMMAND ----------
-- DBTITLE 1,A Python cell that references DBFS
-- MAGIC %python
-- MAGIC DBFS = "dbfs:/..."
-- MAGIC DBFS = "/dbfs/mnt"
-- MAGIC DBFS = "/mnt/"
-- MAGIC DBFS = "dbfs:/..."
-- MAGIC load_data('/dbfs/mnt/data')
-- MAGIC load_data('/data')
-- MAGIC load_data('/dbfs/mnt/data', '/data')
-- MAGIC # load_data('/dbfs/mnt/data', '/data')
-- ucx[direct-filesystem-access:+1:0:+1:34] The use of direct filesystem references is deprecated: /mnt/foo/bar
-- MAGIC spark.read.parquet("/mnt/foo/bar")
-- ucx[direct-filesystem-access:+1:0:+1:39] The use of direct filesystem references is deprecated: dbfs:/mnt/foo/bar
-- MAGIC spark.read.parquet("dbfs:/mnt/foo/bar")
-- ucx[direct-filesystem-access:+1:0:+1:40] The use of direct filesystem references is deprecated: dbfs://mnt/foo/bar
-- MAGIC spark.read.parquet("dbfs://mnt/foo/bar")
-- ucx[direct-filesystem-access:+1:0:+1:24] The use of direct filesystem references is deprecated: dbfs:/...
-- MAGIC spark.read.parquet(DBFS)

-- COMMAND ----------
-- ucx[direct-filesystem-access-in-sql-query:+0:0:+0:1024] The use of direct filesystem references is deprecated: dbfs:/...
-- DBTITLE 1,A SQL cell that references DBFS

SELECT * FROM parquet.`dbfs:/...` LIMIT 10

-- COMMAND ----------
-- ucx[direct-filesystem-access-in-sql-query:+0:0:+0:1024] The use of direct filesystem references is deprecated: /mnt/...
-- DBTITLE 1,A SQL cell that references DBFS
SELECT * FROM delta.`/mnt/...` WHERE foo > 6

-- COMMAND ----------
-- ucx[direct-filesystem-access-in-sql-query:+0:0:+0:1024] The use of direct filesystem references is deprecated: /a/b/c
-- DBTITLE 1,A SQL cell that references DBFS
        SELECT * FROM json.`/a/b/c` WHERE foo > 6

-- COMMAND ----------
-- ucx[direct-filesystem-access-in-sql-query:+0:0:+0:1024] The use of direct filesystem references is deprecated: /...
-- DBTITLE 1,A SQL cell that references DBFS
        DELETE FROM json.`/...` WHERE foo = 'bar'

-- COMMAND ----------
-- ucx[direct-filesystem-access-in-sql-query:+0:0:+0:1024] The use of direct filesystem references is deprecated: /dbfs/...
-- DBTITLE 1,A SQL cell that references DBFS

MERGE INTO delta.`/dbfs/...` t USING source ON t.key = source.key WHEN MATCHED THEN DELETE


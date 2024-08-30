-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Test notebook for DBFS discovery in Notebooks

-- COMMAND ----------
-- DBTITLE 1,A Python cell that references DBFS
-- MAGIC %python
-- ucx[dbfs-usage:+1:7:+1:18] Deprecated file system path: dbfs:/...
-- MAGIC DBFS = "dbfs:/..."
-- ucx[dbfs-usage:+1:7:+1:18] Deprecated file system path: /dbfs/mnt
-- MAGIC DBFS = "/dbfs/mnt"
-- ucx[dbfs-usage:+1:7:+1:14] Deprecated file system path: /mnt/
-- MAGIC DBFS = "/mnt/"
-- ucx[dbfs-usage:+1:7:+1:18] Deprecated file system path: dbfs:/...
-- MAGIC DBFS = "dbfs:/..."
-- ucx[dbfs-usage:+1:10:+1:26] Deprecated file system path: /dbfs/mnt/data
-- MAGIC load_data('/dbfs/mnt/data')
-- MAGIC load_data('/data')
-- ucx[dbfs-usage:+1:10:+1:26] Deprecated file system path: /dbfs/mnt/data
-- MAGIC load_data('/dbfs/mnt/data', '/data')
-- MAGIC # load_data('/dbfs/mnt/data', '/data')
-- ucx[implicit-dbfs-usage:+2:0:+2:34] The use of default dbfs: references is deprecated: /mnt/foo/bar
-- ucx[dbfs-usage:+1:19:+1:33] Deprecated file system path: /mnt/foo/bar
-- MAGIC spark.read.parquet("/mnt/foo/bar")
-- ucx[direct-filesystem-access:+2:0:+2:39] The use of direct filesystem references is deprecated: dbfs:/mnt/foo/bar
-- ucx[dbfs-usage:+1:19:+1:38] Deprecated file system path: dbfs:/mnt/foo/bar
-- MAGIC spark.read.parquet("dbfs:/mnt/foo/bar")
-- ucx[direct-filesystem-access:+2:0:+2:40] The use of direct filesystem references is deprecated: dbfs://mnt/foo/bar
-- ucx[dbfs-usage:+1:19:+1:39] Deprecated file system path: dbfs://mnt/foo/bar
-- MAGIC spark.read.parquet("dbfs://mnt/foo/bar")
-- MAGIC # Would need a stateful linter to detect this next one
-- MAGIC spark.read.parquet(DBFS)

-- COMMAND ----------
-- ucx[dbfs-read-from-sql-query:+0:0:+0:1024] The use of DBFS is deprecated: dbfs:/...
-- DBTITLE 1,A SQL cell that references DBFS

SELECT * FROM parquet.`dbfs:/...` LIMIT 10

-- COMMAND ----------
-- ucx[dbfs-read-from-sql-query:+0:0:+0:1024] The use of DBFS is deprecated: /mnt/...
-- DBTITLE 1,A SQL cell that references DBFS
SELECT * FROM delta.`/mnt/...` WHERE foo > 6

-- COMMAND ----------
-- ucx[dbfs-read-from-sql-query:+0:0:+0:1024] The use of DBFS is deprecated: /a/b/c
-- DBTITLE 1,A SQL cell that references DBFS
        SELECT * FROM json.`/a/b/c` WHERE foo > 6

-- COMMAND ----------
-- ucx[dbfs-read-from-sql-query:+0:0:+0:1024] The use of DBFS is deprecated: /...
-- DBTITLE 1,A SQL cell that references DBFS
        DELETE FROM json.`/...` WHERE foo = 'bar'

-- COMMAND ----------
-- ucx[dbfs-read-from-sql-query:+0:0:+0:1024] The use of DBFS is deprecated: /dbfs/...
-- DBTITLE 1,A SQL cell that references DBFS

MERGE INTO delta.`/dbfs/...` t USING source ON t.key = source.key WHEN MATCHED THEN DELETE


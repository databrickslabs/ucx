-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Test notebook for Use tracking in Notebooks

-- COMMAND ----------
-- DBTITLE 1,A SQL cell that changes the DB

USE different_db

-- COMMAND ----------
-- ucx[table-migrated-to-uc-sql:+0:0:+0:1024] Table different_db.testtable is migrated to cata2.newspace.table in Unity Catalog
-- DBTITLE 1,A SQL cell that references tables

SELECT * FROM  testtable LIMIT 10

-- COMMAND ----------
-- DBTITLE 1,A SQL cell that changes the DB to one we migrate from

USE old

-- COMMAND ----------
-- ucx[table-migrated-to-uc-sql:+0:0:+0:1024] Table old.testtable is migrated to cata3.newspace.table in Unity Catalog
-- DBTITLE 1,A SQL cell that references tables

SELECT * FROM  testtable LIMIT 10

-- COMMAND ----------
-- ucx[table-migrated-to-uc-sql:+0:0:+0:1024] Table old.stuff is migrated to brand.new.things in Unity Catalog
-- DBTITLE 1,A SQL cell that references tables

SELECT * FROM  stuff LIMIT 10

-- COMMAND ----------
-- DBTITLE 1,A Python cell that uses calls to change the USE
-- MAGIC %python
-- MAGIC # This is a Python cell that uses calls to change the USE...

spark.sql("use different_db")

-- COMMAND ----------
-- ucx[table-migrated-to-uc-sql:+0:0:+0:1024] Table different_db.testtable is migrated to cata2.newspace.table in Unity Catalog
-- DBTITLE 1,A SQL cell that references DBFS

SELECT * FROM testtable LIMIT 10

-- COMMAND ----------
-- ucx[table-migrated-to-uc-sql:+0:0:+0:1024] Table old.testtable is migrated to cata3.newspace.table in Unity Catalog
-- DBTITLE 1,A SQL cell that references DBFS

SELECT * FROM old.testtable LIMIT 10

-- COMMAND ----------
-- DBTITLE 1,A SQL cell that changes the DB to the default

USE default

-- COMMAND ----------
-- ucx[table-migrated-to-uc-sql:+0:0:+0:1024] Table default.testtable is migrated to cata.nondefault.table in Unity Catalog
-- DBTITLE 1,A SQL cell that references DBFS

SELECT * FROM testtable LIMIT 10

-- COMMAND ----------
-- DBTITLE 1,A SQL cell that references tables

MERGE INTO catalog.schema.testtable t USING source ON t.key = source.key WHEN MATCHED THEN DELETE


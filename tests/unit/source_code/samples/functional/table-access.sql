-- Databricks notebook source

SELECT * FROM b.c LEFT JOIN c.d USING (e)

-- COMMAND ----------

-- MAGIC %python
-- ucx[default-format-changed-in-dbr8:+1:0:+1:18] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
-- MAGIC spark.table("a.b").count()
-- MAGIC spark.sql("SELECT * FROM b.c LEFT JOIN c.d USING (e)")



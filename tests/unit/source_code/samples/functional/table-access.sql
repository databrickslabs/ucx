-- Databricks notebook source

SELECT * FROM b.c LEFT JOIN c.d USING (e)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.table("a.b").count()
-- MAGIC spark.sql("SELECT * FROM b.c LEFT JOIN c.d USING (e)")



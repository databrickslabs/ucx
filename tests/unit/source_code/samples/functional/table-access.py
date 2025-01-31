# Databricks notebook source
spark.table("a.b").count()
spark.sql("SELECT * FROM b.c LEFT JOIN c.d USING (e)")
%sql SELECT * FROM b.c LEFT JOIN c.d USING (e)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM b.c LEFT JOIN c.d USING (e)


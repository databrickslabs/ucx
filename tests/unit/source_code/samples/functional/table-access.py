# Databricks notebook source
# ucx[default-format-changed-in-dbr8:+1:0:+1:18] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
spark.table("a.b").count()
spark.sql("SELECT * FROM b.c LEFT JOIN c.d USING (e)")
%sql SELECT * FROM b.c LEFT JOIN c.d USING (e)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM b.c LEFT JOIN c.d USING (e)


# Databricks notebook source

some_table_name = "table"

# COMMAND ----------

%run ./_child_that_uses_missing_value.py

# COMMAND ----------

# ucx[default-format-changed-in-dbr8:+1:0:+1:37] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
spark.table(f"{some_table_name}_{x}")  # x is introduced into scope by child notebook

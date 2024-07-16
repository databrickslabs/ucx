# Databricks notebook source

dbutils.notebook.run("./values_across_notebooks_child.py")
# ucx[default-format-changed-in-dbr8:+3:0:+3:19] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
# dbutils.notebook.run runs in a separate process and thus does NOT import values
# ucx[cannot-autofix-table-reference:+1:0:+1:19] Can't migrate 'spark.table(f'{a}')' because its table name argument cannot be computed
spark.table(f"{a}")

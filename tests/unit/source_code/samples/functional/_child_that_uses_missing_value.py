# Databricks notebook source

# ucx[cannot-autofix-table-reference:+2:0:+2:33] Can't migrate 'spark.table(f'{some_table_name}')' because its table name argument cannot be computed
# ucx[default-format-changed-in-dbr8:+1:0:+1:33] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
spark.table(f"{some_table_name}")

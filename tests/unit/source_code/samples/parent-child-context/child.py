# Databricks notebook source

# ucx[default-format-changed-in-dbr8:+1:0:+1:33] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
spark.table(f"{some_table_name}")

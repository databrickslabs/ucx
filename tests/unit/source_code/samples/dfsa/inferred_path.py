# Databricks notebook source
path = "s3a://prefix/some_inferred_file.csv"

# COMMAND ----------

spark.read.format("delta").load(path)

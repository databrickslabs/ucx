# Databricks notebook source

some_table_name = "some_table_name"


# COMMAND ----------

dbutils.notebook.run("./_child_that_uses_missing_value.py")

# COMMAND ----------

other_table_name = "other_table_name"

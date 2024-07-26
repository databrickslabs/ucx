# Databricks notebook source

some_table_name = "old.thing"


# COMMAND ----------

dbutils.notebook.run("./_child_that_uses_missing_value.py")

# COMMAND ----------

other_table_name = "old.thing"

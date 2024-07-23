# Databricks notebook source

some_table_name = "some_table_name"


# COMMAND ----------

%run ./_child_that_uses_value_from_parent.py

# COMMAND ----------

other_table_name = "other_table_name"

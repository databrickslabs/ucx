# Databricks notebook source

some_table_name = "old.thing"


# COMMAND ----------

%run ./_child_that_uses_value_from_parent.py

# COMMAND ----------

other_table_name = "old.thing"

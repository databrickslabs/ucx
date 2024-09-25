# Databricks notebook source

# pylint: disable=dead-code,import-error

# COMMAND ----------

# MAGIC %md
# MAGIC This is a Python notebook, and it can be run as a python script.
# MAGIC However it includes a cell that is marked as Python instead of just being a default piece of Python script.
# MAGIC A notebook-aware processor will see the special python cell, whereas an unaware processor (such as Python)
# MAGIC will ignore it.

# COMMAND ----------

# MAGIC %python
# MAGIC import greenlet
# MAGIC raise RuntimeError("Notebook-aware handling.")

# COMMAND ----------

from pyspark.sql import SparkSession  # type: ignore[import-not-found]


def main(*_argv: str):
    spark = SparkSession.builder.getOrCreate()
    print(f"Spark version: {spark.version}")


if __name__ == '__main__':
    import sys

    main(*sys.argv)

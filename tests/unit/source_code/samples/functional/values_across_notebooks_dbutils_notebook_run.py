# Databricks notebook source

dbutils.notebook.run("./values_across_notebooks_child.py")
# dbutils.notebook.run runs in a separate process and thus does NOT import values
# ucx[cannot-autofix-table-reference:+1:0:+1:19] Can't migrate 'spark.table(f'{a}')' because its table name argument cannot be computed
spark.table(f"{a}")

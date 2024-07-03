# Databricks notebook source
a = "./leaf1.py"
dbutils.notebook.run(a)
b = some_function()
# ucx[notebook-run-cannot-compute-value:+1:0:+1:23] Path for 'dbutils.notebook.run' cannot be computed and requires adjusting the notebook path(s)
dbutils.notebook.run(b)

# Databricks notebook source
a = "./leaf1.py"
# ucx[dbutils-notebook-run-literal:+1:0:+1:23] Call to 'dbutils.notebook.run' will be migrated automatically
dbutils.notebook.run(a)
b = some_function()
# ucx[dbutils-notebook-run-dynamic:+1:0:+1:23] Path for 'dbutils.notebook.run' cannot be computed and requires adjusting the notebook path(s)
dbutils.notebook.run(b)

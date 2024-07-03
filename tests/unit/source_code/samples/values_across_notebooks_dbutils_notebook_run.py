# Databricks notebook source

dbutils.notebook.run("./values_across_notebooks_child.py")
spark.table(f"{a}")

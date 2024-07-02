# Databricks notebook source
from values_across_imports_child import config
spark.table(f"{config['Hi']}")

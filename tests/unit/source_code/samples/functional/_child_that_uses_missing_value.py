# Databricks notebook source

# ucx[cannot-autofix-table-reference:+1:0:+1:33] Can't migrate 'spark.table(f'{some_table_name}')' because its table name argument cannot be computed
spark.table(f"{some_table_name}")
x = 2

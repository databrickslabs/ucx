# Databricks notebook source

a = 12

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE hive_metastore.indices_historical_data.sp_500 LOCATION 's3a://db-gtm-industry-solutions/data/fsi/capm/sp_500/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE hive_metastore.indices_historical_data.sp_550 LOCATION 's3a://db-gtm-industry-solutions/data/fsi/capm/sp_550/'

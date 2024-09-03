# Databricks notebook source

a = 12

# COMMAND ----------

# ucx[direct-filesystem-access-in-sql-query:+0:0:+0:1024] The use of direct filesystem references is deprecated: s3a://db-gtm-industry-solutions/data/fsi/capm/sp_500/
# MAGIC %sql
# MAGIC CREATE TABLE hive_metastore.indices_historical_data.sp_500 LOCATION 's3a://db-gtm-industry-solutions/data/fsi/capm/sp_500/'

# COMMAND ----------

# ucx[direct-filesystem-access-in-sql-query:+0:0:+0:1024] The use of direct filesystem references is deprecated: s3a://db-gtm-industry-solutions/data/fsi/capm/sp_550/
# MAGIC %sql
# MAGIC CREATE TABLE hive_metastore.indices_historical_data.sp_550 LOCATION 's3a://db-gtm-industry-solutions/data/fsi/capm/sp_550/'

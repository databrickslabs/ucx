-- Databricks notebook source
-- ucx[direct-filesystem-access-in-sql-query:+0:0:+0:1024] The use of direct filesystem references is deprecated: dbfs://examples/src/main/resources/users.parquet
SELECT * FROM parquet.`dbfs://examples/src/main/resources/users.parquet`

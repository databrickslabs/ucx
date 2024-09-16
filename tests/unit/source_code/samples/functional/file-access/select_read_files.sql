-- Databricks notebook source
-- ucx[direct-filesystem-access-in-sql-query:+0:0:+0:1024] The use of direct filesystem references is deprecated: s3a://db-gtm-industry-solutions/data/fsi/capm/sp_500/file.csv
SELECT * FROM read_files("s3a://db-gtm-industry-solutions/data/fsi/capm/sp_500/file.csv") LIMIT 10

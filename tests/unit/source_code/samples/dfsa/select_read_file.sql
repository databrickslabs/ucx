-- Databricks notebook source
SELECT * FROM read_files("s3a://db-gtm-industry-solutions/data/fsi/capm/sp_500/file.csv") LIMIT 10

-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE pcmd_stream_bronze
COMMENT "PCMD Stream - Bronze"
AS SELECT *
  FROM cloud_files(
    "s3a://db-gtm-industry-solutions/data/CME/telco/PCMD",
    "json",
    map(
      "header", "false",
      "mergeSchema", "true",
      "cloudFiles.inferColumnTypes", "true"
    )
  )

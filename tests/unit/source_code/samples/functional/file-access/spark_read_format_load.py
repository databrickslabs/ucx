# Databricks notebook source
# ucx[direct-filesystem-access:+1:0:+1:61] The use of direct filesystem references is deprecated: s3a://prefix/some_file.csv
spark.read.format("delta").load("s3a://prefix/some_file.csv")

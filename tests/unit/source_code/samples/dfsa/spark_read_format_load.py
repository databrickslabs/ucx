# Databricks notebook source
spark.read.format("delta").load("s3a://prefix/some_file.csv")

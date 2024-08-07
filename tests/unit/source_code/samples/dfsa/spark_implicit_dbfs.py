# Databricks notebook source
spark.read.format("delta").load("/prefix/some_file.csv")

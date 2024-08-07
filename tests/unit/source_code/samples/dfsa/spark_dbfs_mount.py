# Databricks notebook source
spark.read.format("delta").load("/mnt/some_file.csv")

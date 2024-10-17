# Databricks notebook source
# ucx[session-state] {"data_security_mode": "USER_ISOLATION"}

# ucx[direct-filesystem-access-in-sql-query:+1:0:+1:86] The use of direct filesystem references is deprecated: /foo/bar
spark.sql("SELECT * FROM db.table LEFT JOIN delta.`/foo/bar` AS t ON t.id = table.id").show()

# ucx[rdd-in-shared-clusters:+2:29:+2:42] RDD APIs are not supported on Unity Catalog clusters in Shared access mode. Rewrite it using DataFrame API
# ucx[legacy-context-in-shared-clusters:+1:29:+1:40] sc is not supported on Unity Catalog clusters in Shared access mode. Rewrite it using spark
rdd2 = spark.createDataFrame(sc.emptyRDD(), 'foo')

import pyspark.sql.functions as F

# ucx[default-format-changed-in-dbr8:+1:17:+1:41] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
churn_features = spark.table("something")
churn_features = (churn_features.withColumn("random", F.rand(seed=42)).withColumn("split",F.when(F.col("random") < train_ratio, "train").when(F.col("random") < train_ratio + val_ratio, "validate").otherwise("test")).drop("random"))

# ucx[default-format-changed-in-dbr8:+1:1:+1:109] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
(churn_features.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("mlops_churn_training"))

# ucx[default-format-changed-in-dbr8:+1:21:+1:74] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
sdf_system_columns = spark.read.table("system.information_schema.columns")

# ucx[sql-parse-error:+1:14:+1:140] SQL expression is not supported yet: SELECT 1 AS col1, 2 AS col2, 3 AS col3 FROM {sdf_system_columns} LIMIT 5
sdf_example = spark.sql("SELECT 1 AS col1, 2 AS col2, 3 AS col3 FROM {sdf_system_columns} LIMIT 5", sdf_system_columns = sdf_system_columns)

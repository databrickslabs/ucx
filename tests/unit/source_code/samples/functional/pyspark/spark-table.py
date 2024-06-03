# ucx[direct-filesystem-access:+1:0:+1:0] The use of direct filesystem references is deprecated: s3://bucket/path
spark.read.csv("s3://bucket/path")
for i in range(10):
    ## DISABLED due to false positives:
    ##  - table-migrate: The default format changed in Databricks Runtime 8.0, from Parquet to Delta
    ## ucx[table-migrate:+1:0:+1:0] Table old.things is migrated to brand.new.stuff in Unity Catalog
    #df = spark.table("old.things")
    do_stuff_with(df)

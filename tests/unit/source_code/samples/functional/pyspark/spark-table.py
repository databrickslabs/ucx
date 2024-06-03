# ucx[direct-filesystem-access:+1:0:+1:0] The use of direct filesystem references is deprecated: s3://bucket/path
spark.read.csv("s3://bucket/path")
for i in range(10):

    ## Check a literal reference to a known table that is migrated.
    ## DISABLED due to false positives:
    ##  - table-migrate: The default format changed in Databricks Runtime 8.0, from Parquet to Delta
    ## ucx[table-migrate:+1:0:+1:0] Table old.things is migrated to brand.new.stuff in Unity Catalog
    #df = spark.table("old.things")
    do_stuff_with(df)

    ## Check a literal reference to an unknown table (that is not migrated); we expect no warning.
    ## DISABLED due to false positives:
    ##  - table-migrate: The default format changed in Databricks Runtime 8.0, from Parquet to Delta
    # df = spark.table("table.we.know.nothing.about")
    do_stuff_with(df)

    ## Check that a call with too many positional arguments is ignored as (presumably) something else; we expect no warning.
    ## DISABLED due to false positives:
    ##  - table-migrate: The default format changed in Databricks Runtime 8.0, from Parquet to Delta
    df = spark.table("old.things", "extra-argument")
    do_stuff_with(df)

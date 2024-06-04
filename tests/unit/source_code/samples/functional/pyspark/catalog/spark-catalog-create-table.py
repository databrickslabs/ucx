# ucx[direct-filesystem-access:+1:0:+1:0] The use of direct filesystem references is deprecated: s3://bucket/path
spark.read.csv("s3://bucket/path")
for i in range(10):

    ## Check a literal reference to a known table that is migrated.
    # ucx[table-migrate:+3:0:+3:0] Table old.things is migrated to brand.new.stuff in Unity Catalog
    # TODO: Implement missing migration warning (on the source argument):
    # #ucx[table-migrate:+1:0:+1:0] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
    df = spark.catalog.createTable("old.things")
    do_stuff_with(df)

    ## Check a literal reference to an unknown table (that is not migrated); we expect no warning.
    # TODO: Implement missing migration warning (on the source argument):
    # #ucx[table-migrate:+1:0:+1:0] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
    df = spark.catalog.createTable("table.we.know.nothing.about")
    do_stuff_with(df)

    ## Check that a call with too many positional arguments is ignored as (presumably) something else; we expect no warning.
    # FIXME: This is a false positive due to an error in the matching specification; only 5 positional args are allowed.
    # ucx[table-migrate:+1:0:+1:0] Table old.things is migrated to brand.new.stuff in Unity Catalog
    df = spark.catalog.createTable("old.things", None, None, None, None, "extra-argument")
    do_stuff_with(df)

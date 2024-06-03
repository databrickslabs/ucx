# ucx[direct-filesystem-access:+1:0:+1:0] The use of direct filesystem references is deprecated: s3://bucket/path
spark.read.csv("s3://bucket/path")
for i in range(10):

    ## Check a literal reference to a known table that is migrated.
    # ucx[table-migrate:+1:0:+1:0] Table old.things is migrated to brand.new.stuff in Unity Catalog
    spark.catalog.refreshTable("old.things")

    ## Check a literal reference to an unknown table (that is not migrated); we expect no warning.
    spark.catalog.refreshTable("table.we.know.nothing.about")

    ## Check that a call with too many positional arguments is ignored as (presumably) something else; we expect no warning.
    spark.catalog.refreshTable("old.things", "extra-argument")

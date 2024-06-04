# ucx[direct-filesystem-access:+1:0:+1:0] The use of direct filesystem references is deprecated: s3://bucket/path
spark.read.csv("s3://bucket/path")
for i in range(10):

    ## Check a literal reference to a known table that is migrated.
    # ucx[table-migrate:+1:0:+1:0] Table old.things is migrated to brand.new.stuff in Unity Catalog
    cached_previously = spark.catalog.isCached("old.things")

    ## Check a literal reference to an unknown table (that is not migrated); we expect no warning.
    cached_previously = spark.catalog.isCached("table.we.know.nothing.about")

    ## Check that a call with too many positional arguments is ignored as (presumably) something else; we expect no warning.
    cached_previously = spark.catalog.isCached("old.things", "extra-argument")

    ## Some calls that use a variable whose value is unknown: they could potentially reference a migrated table.
    # ucx[table-migrate:+1:0:+1:0] Can't migrate 'isCached' because its table name argument is not a constant
    cached_previously = spark.catalog.isCached(name)
    # ucx[table-migrate:+1:0:+1:0] Can't migrate 'isCached' because its table name argument is not a constant
    cached_previously = spark.catalog.isCached(f"boop{stuff}")

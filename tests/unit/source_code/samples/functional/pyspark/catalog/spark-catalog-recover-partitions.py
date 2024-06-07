# ucx[direct-filesystem-access:+1:0:+1:34] The use of direct filesystem references is deprecated: s3://bucket/path
spark.read.csv("s3://bucket/path")
for i in range(10):

    ## Check a literal reference to a known table that is migrated.
    # ucx[table-migrate:+1:4:+1:49] Table old.things is migrated to brand.new.stuff in Unity Catalog
    spark.catalog.recoverPartitions("old.things")

    ## Check a literal reference to an unknown table (that is not migrated); we expect no warning.
    spark.catalog.recoverPartitions("table.we.know.nothing.about")

    ## Check that a call with too many positional arguments is ignored as (presumably) something else; we expect no warning.
    spark.catalog.recoverPartitions("old.things", "extra-argument")

    ## Some calls that use a variable whose value is unknown: they could potentially reference a migrated table.
    # ucx[table-migrate:+1:4:+1:41] Can't migrate 'recoverPartitions' because its table name argument is not a constant
    spark.catalog.recoverPartitions(name)
    # ucx[table-migrate:+1:4:+1:51] Can't migrate 'recoverPartitions' because its table name argument is not a constant
    spark.catalog.recoverPartitions(f"boop{stuff}")

    ## Some trivial references to the method or table in unrelated contexts that should not trigger warnigns.
    # FIXME: This is a false positive; any method named 'recoverPartitions' is triggering the warning.
    # ucx[table-migrate:+1:4:+1:50] Table old.things is migrated to brand.new.stuff in Unity Catalog
    something_else.recoverPartitions("old.things")
    a_function("old.things")

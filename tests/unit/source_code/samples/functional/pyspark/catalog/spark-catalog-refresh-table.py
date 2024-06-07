# ucx[direct-filesystem-access:+1:0:+1:34] The use of direct filesystem references is deprecated: s3://bucket/path
spark.read.csv("s3://bucket/path")
for i in range(10):

    ## Check a literal reference to a known table that is migrated.
    # ucx[table-migrate:+1:4:+1:44] Table old.things is migrated to brand.new.stuff in Unity Catalog
    spark.catalog.refreshTable("old.things")

    ## Check a literal reference to an unknown table (that is not migrated); we expect no warning.
    spark.catalog.refreshTable("table.we.know.nothing.about")

    ## Check that a call with too many positional arguments is ignored as (presumably) something else; we expect no warning.
    spark.catalog.refreshTable("old.things", "extra-argument")

    ## Some calls that use a variable whose value is unknown: they could potentially reference a migrated table.
    # ucx[table-migrate:+1:4:+1:36] Can't migrate 'refreshTable' because its table name argument is not a constant
    spark.catalog.refreshTable(name)
    # ucx[table-migrate:+1:4:+1:46] Can't migrate 'refreshTable' because its table name argument is not a constant
    spark.catalog.refreshTable(f"boop{stuff}")

    ## Some trivial references to the method or table in unrelated contexts that should not trigger warnigns.
    # FIXME: This is a false positive; any method named 'refreshTable' is triggering the warning.
    # ucx[table-migrate:+1:4:+1:45] Table old.things is migrated to brand.new.stuff in Unity Catalog
    something_else.refreshTable("old.things")
    a_function("old.things")

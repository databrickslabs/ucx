# ucx[direct-filesystem-access:+1:0:+1:34] The use of direct filesystem references is deprecated: s3://bucket/path
spark.read.csv("s3://bucket/path")
for i in range(10):

    ## Check a literal reference to a known table that is migrated.
    # ucx[table-migrated-to-uc:+1:4:+1:44] Table old.things is migrated to brand.new.stuff in Unity Catalog
    spark.catalog.refreshTable("old.things")

    ## Check a literal reference to an unknown table (that is not migrated); we expect no warning.
    spark.catalog.refreshTable("table.we.know.nothing.about")

    ## Check that a call with too many positional arguments is ignored as (presumably) something else; we expect no warning.
    spark.catalog.refreshTable("old.things", "extra-argument")

    ## Some calls that use a variable whose value is unknown: they could potentially reference a migrated table.
    # ucx[cannot-autofix-table-reference:+1:4:+1:36] Can't migrate 'spark.catalog.refreshTable(name)' because its table name argument cannot be computed
    spark.catalog.refreshTable(name)
    # ucx[cannot-autofix-table-reference:+1:4:+1:46] Can't migrate 'spark.catalog.refreshTable(f'boop{stuff}')' because its table name argument cannot be computed
    spark.catalog.refreshTable(f"boop{stuff}")

    ## Some trivial references to the method or table in unrelated contexts that should not trigger warnigns.
    something_else.refreshTable("old.things")
    a_function("old.things")

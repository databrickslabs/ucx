# ucx[direct-filesystem-access:+1:0:+1:34] The use of direct filesystem references is deprecated: s3://bucket/path
spark.read.csv("s3://bucket/path")
for i in range(10):

    ## Check a literal reference to a known table that is migrated.
    # ucx[table-migrate:+1:12:+1:48] Table old.things is migrated to brand.new.stuff in Unity Catalog
    table = spark.catalog.getTable("old.things")
    do_stuff_with(table)

    ## Check a literal reference to an unknown table (that is not migrated); we expect no warning.
    table = spark.catalog.getTable("table.we.know.nothing.about")
    do_stuff_with(table)

    ## Check that a call with too many positional arguments is ignored as (presumably) something else; we expect no warning.
    table = spark.catalog.getTable("old.things", "extra-argument")
    do_stuff_with(table)

    ## Some calls that use a variable whose value is unknown: they could potentially reference a migrated table.
    # ucx[table-migrate-cannot-compute-value:+1:12:+1:40] Can't migrate 'spark.catalog.getTable(name)' because its table name argument cannot be computed
    table = spark.catalog.getTable(name)
    do_stuff_with(table)
    # ucx[table-migrate-cannot-compute-value:+1:12:+1:50] Can't migrate 'spark.catalog.getTable(f'boop{stuff}')' because its table name argument cannot be computed
    table = spark.catalog.getTable(f"boop{stuff}")
    do_stuff_with(table)

    ## Some trivial references to the method or table in unrelated contexts that should not trigger warnigns.
    # FIXME: This is a false positive; any method named 'getTable' is triggering the warning.
    # ucx[table-migrate:+1:4:+1:41] Table old.things is migrated to brand.new.stuff in Unity Catalog
    something_else.getTable("old.things")
    a_function("old.things")
